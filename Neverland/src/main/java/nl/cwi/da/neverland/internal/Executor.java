package nl.cwi.da.neverland.internal;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import nl.cwi.da.neverland.internal.Scheduler.SubquerySchedule;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.stat.descriptive.rank.Percentile;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class Executor {

	public abstract List<ResultSet> executeSchedule(
			Scheduler.SubquerySchedule schedule) throws NeverlandException;

	public static class ReschedulingExecutor extends MultiThreadedExecutor {

		private static Logger log = Logger
				.getLogger(ReschedulingExecutor.class);

		public ReschedulingExecutor(int executorThreads, int connectionsPerNode) {
			super(executorThreads, connectionsPerNode);
		}

		Random rand = new Random(System.currentTimeMillis());

		private NeverlandNode randomNodeFromSchedule(SubquerySchedule schedule,
				Collection<NeverlandNode> ignoreNodes) {
			NeverlandNode[] nodesArr = schedule.keySet().toArray(
					new NeverlandNode[0]);
			NeverlandNode nn = null;
			do {
				nn = nodesArr[rand.nextInt(nodesArr.length)];
			} while (ignoreNodes.contains(nn));
			return nn;
		}

		@Override
		public List<ResultSet> executeSchedule(SubquerySchedule schedule)
				throws NeverlandException {

			long subqueries = 0;
			List<Future<SubqueryResultSet>> resultSetsFutures = new ArrayList<Future<SubqueryResultSet>>();
			List<ResultSet> resultSets = new ArrayList<ResultSet>();
			Map<Subquery, Long> subqueryStart = new HashMap<Subquery, Long>();
			Collection<Double> responseTimes = new ArrayList<Double>();
			log.info("Running schedule for #" + schedule.getQuery().getId()
					+ ", " + schedule.numSubqueries() + " Subqueries scheduled");

			// schedule everything
			for (Entry<NeverlandNode, List<Subquery>> sentry : schedule
					.entrySet()) {
				final NeverlandNode nn = sentry.getKey();
				// right, so now execute all the queries
				for (final Subquery sq : sentry.getValue()) {
					subqueries++;
					resultSetsFutures.add(schedule(nn, sq,
							schedule.getTimeoutMs()));
					subqueryStart.put(sq, System.currentTimeMillis());
				}
			}
			long round = 0;
			// now we play the waiting game
			boolean finishedQuorum = false;
			do {
				// check and cleanup finished subqueries

				Iterator<Future<SubqueryResultSet>> futureIterator = resultSetsFutures
						.iterator();
				while (futureIterator.hasNext()) {
					Future<SubqueryResultSet> rf = futureIterator.next();
					if (rf.isDone()) {
						futureIterator.remove();
						try {
							SubqueryResultSet srs = rf.get();
							if (srs == null) {
								throw new NeverlandException(
										"null result set. Bah.");
							}
							// only add result set if we are still waiting
							// for it
							// if it is not on the todo list, another
							// attempt has already
							// delivered the result and we discard it.
							if (subqueryStart.containsKey(srs.sq)) {
								resultSets.add(srs.rs);
								responseTimes.add(System.currentTimeMillis()
										- subqueryStart.get(srs.sq) * 1.0);
								subqueryStart.remove(srs.sq);
							}
						} catch (Exception e) {
							log.warn(e);
						}
					}
				}

				// check percentage of result sets already delivered
				double finishedPercent = (subqueries - subqueryStart.size())
						* 1.0 / subqueries;
				if (finishedPercent > Constants.RESCHEDULER_FINISHED_QUORUM) {
					finishedQuorum = true;
				}

				double responseTimeLimit = new Percentile().evaluate(ArrayUtils
						.toPrimitive(responseTimes.toArray(new Double[0])),
						Constants.RESCHEDULER_FINISHED_QUORUM * 100);

				if (log.isDebugEnabled())
					log.debug("Finishing quorum: " + subqueryStart.size()
							+ " of " + subqueries + " running ("
							+ finishedPercent * 100 + "%, quorum reached="
							+ finishedQuorum + ", response time limit="
							+ responseTimeLimit + ")");

				// if we have reached the response quorum we start
				// rescheduling
				if (finishedQuorum) {
					for (Entry<Subquery, Long> se : subqueryStart.entrySet()) {
						Subquery sq = se.getKey();
						double queryRuntime = System.currentTimeMillis()
								- se.getValue();
						// if a query takes longer than the time limit,
						// reschedule
						if (queryRuntime > responseTimeLimit) {
							NeverlandNode oldNode = schedule.getNode(sq);
							NeverlandNode newNode = randomNodeFromSchedule(
									schedule, Arrays.asList(oldNode));
							log.info(round + " Rescheduling Subquery "
									+ sq.getId() + " from "
									+ oldNode.getHostname() + " to "
									+ newNode.getHostname());

							// fix the schedule for bookkeeping reasons
							schedule.reschedule(oldNode, sq, newNode);
							// overwrite the start time with the current
							// time
							resultSetsFutures.add(schedule(newNode, sq,
									schedule.getTimeoutMs()));
							subqueryStart.put(sq, System.currentTimeMillis());
						}
					}
				}
				try {
					Thread.sleep(Constants.RESCHEDULER_SLEEP_MS);
				} catch (InterruptedException e) {
					// don't care
				}
				round++;
				// repeat until we have all enough result sets
			} while (resultSets.size() < subqueries);

			// if we get and do not have enough result sets, very bad!
			if (resultSets.size() != subqueries) {
				throw new NeverlandException("Not enough result sets, need"
						+ subqueries + ", have " + resultSets.size());
			}
			return resultSets;
		}
	}

	public static class MultiThreadedExecutor extends Executor {
		private Map<String, ComboPooledDataSource> dataSources = new HashMap<String, ComboPooledDataSource>();
		private static Logger log = Logger
				.getLogger(MultiThreadedExecutor.class);

		static class SubqueryResultSet {
			protected Subquery sq;
			protected InternalResultSet rs;
		}

		private int connectionsPerNode;
		private ExecutorService executorService;

		public MultiThreadedExecutor(int executorThreads, int connectionsPerNode) {
			this.connectionsPerNode = connectionsPerNode;
			this.executorService = Executors
					.newFixedThreadPool(executorThreads);
		}

		ScheduledExecutorService canceller = Executors
				.newSingleThreadScheduledExecutor();

		protected <T> Future<T> executeTask(Callable<T> c, long timeoutMS) {
			final Future<T> future = executorService.submit(c);
			canceller.schedule(new Callable<Void>() {
				public Void call() {
					future.cancel(true);
					return null;
				}
			}, timeoutMS, TimeUnit.MILLISECONDS);
			return future;
		}

		protected Future<SubqueryResultSet> schedule(final NeverlandNode nn,
				final Subquery sq, long timeoutMs) {
			synchronized (this) {
				if (!dataSources.containsKey(nn.getId())) {
					log.debug("Opening new connection pool to "
							+ nn.getJdbcUrl());
					ComboPooledDataSource cpds = new ComboPooledDataSource();
					try {
						cpds.setDriverClass(nn.getJdbcDriver());
					} catch (PropertyVetoException e) {
						log.warn("Unable to load JDBC driver", e);
					}

					// JDBC login config, as advertised by worker
					cpds.setJdbcUrl(nn.getJdbcUrl());
					cpds.setUser(nn.getJdbcUser());
					cpds.setPassword(nn.getJdbcPass());

					// some config, rather arbitrary. however, number cpus?
					cpds.setMinPoolSize(connectionsPerNode / 2);
					cpds.setAcquireIncrement(1);
					cpds.setMaxPoolSize(connectionsPerNode);

					// TODO: investigate here...
					cpds.setNumHelperThreads(connectionsPerNode);
					dataSources.put(nn.getId(), cpds);
				}
			}
			final ComboPooledDataSource cpds = dataSources.get(nn.getId());

			return executeTask(new Callable<SubqueryResultSet>() {
				@Override
				public SubqueryResultSet call() throws Exception {

					SubqueryResultSet crs = null;
					Connection c = null;
					Statement s = null;
					ResultSet rs = null;

					try {
						long connStart = System.currentTimeMillis();
						c = cpds.getConnection();
						s = c.createStatement();
						sq.setConnectTimeSecs((System.currentTimeMillis() - connStart) / 1000.0);

						long subqueryStart = System.currentTimeMillis();

						rs = s.executeQuery(sq.getSql());
						crs = new SubqueryResultSet();
						crs.rs = new InternalResultSet(rs);
						crs.sq = sq;

						sq.setTimeTaken((System.currentTimeMillis() - subqueryStart) / 1000.0);
						sq.setAssignedNode(nn);
						sq.setResultSetSize(crs.rs.size());

						crs.rs.beforeFirst();

						log.debug("Got result on " + sq.getSql() + ":"
								+ sq.getSlice() + " from " + nn.getHostname());

					} catch (SQLException e) {
						log.warn(e);
						e.printStackTrace();
						// TODO: recover from failed node,
						// schedule subquery to someone else

					} finally {
						try {
							rs.close();
						} catch (SQLException se) {
							log.warn(se);
						}
						try {
							s.close();
						} catch (SQLException se) {
							log.warn(se);
						}
						try {
							c.close();
						} catch (SQLException se) {
							log.warn(se);
						}
					}
					return crs;
				}
			}, timeoutMs);

		}

		@Override
		public List<ResultSet> executeSchedule(final SubquerySchedule schedule)
				throws NeverlandException {
			long subqueries = 0;
			List<Future<SubqueryResultSet>> resultSetsFutures = new ArrayList<Future<SubqueryResultSet>>();
			List<ResultSet> resultSets = new ArrayList<ResultSet>();

			log.info("Running schedule for #" + schedule.getQuery().getId()
					+ ", " + schedule.numSubqueries() + " Subqueries scheduled");
			long queryStart = System.currentTimeMillis();

			for (Entry<NeverlandNode, List<Subquery>> sentry : schedule
					.entrySet()) {
				final NeverlandNode nn = sentry.getKey();

				// right, so now execute all the queries
				for (final Subquery sq : sentry.getValue()) {
					subqueries++;
					resultSetsFutures.add(schedule(nn, sq,
							schedule.getTimeoutMs()));
				}
			}

			for (Future<SubqueryResultSet> rf : resultSetsFutures) {
				try {
					ResultSet r = rf.get().rs;
					if (r != null) {
						resultSets.add(r);
					}
				} catch (Exception e) {
					log.warn(e);
				}
			}

			if (resultSets.size() != subqueries) {
				throw new NeverlandException(
						"Not enough result sets found to combine, something must have gone wrong.");
			}
			schedule.getQuery().setTimeTaken(
					(System.currentTimeMillis() - queryStart) / 1000.0);
			return resultSets;
		}
	}
}
