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
			NeverlandNode[] nodesArr = (NeverlandNode[]) schedule.keySet()
					.toArray();
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
			Map<Subquery, NeverlandNode> subqueryTodo = new HashMap<Subquery, NeverlandNode>();

			log.info("Running schedule for #" + schedule.getQuery().getId()
					+ ", " + schedule.size() + " Subqueries");

			for (Entry<NeverlandNode, List<Subquery>> sentry : schedule
					.entrySet()) {
				final NeverlandNode nn = sentry.getKey();
				// right, so now execute all the queries
				for (final Subquery sq : sentry.getValue()) {
					subqueries++;
					resultSetsFutures.add(schedule(nn, sq,
							schedule.getTimeoutMs()));
					subqueryTodo.put(sq, nn);
				}

				boolean finishedQuorum = false;
				do {

					// check percentage of result sets already delivered
					double finishedPercent = subqueryTodo.size() / subqueries;
					if (finishedPercent > Constants.RESCHEDULER_FINISHED_QUORUM) {
						finishedQuorum = true;
					}
					if (log.isDebugEnabled()) {
						log.debug("Reached finishing quorum of "
								+ Constants.RESCHEDULER_FINISHED_QUORUM * 100
								+ "% with " + subqueryTodo.size() + " of "
								+ subqueries + "(" + finishedPercent * 100
								+ "%, quorum finished=)" + finishedQuorum);
					}

					// check and cleanup finished subqueries
					for (Future<SubqueryResultSet> rf : resultSetsFutures) {
						if (rf.isDone()) {
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
								if (subqueryTodo.containsKey(srs.sq)) {
									resultSets.add(srs.rs);
									subqueryTodo.remove(srs.sq);
								}
							} catch (Exception e) {
								log.warn(e);
							}
						}
					}
					// everything still running as the quorum is reached will be
					// rescheduled
					if (finishedQuorum) {
						for (Entry<Subquery, NeverlandNode> se : subqueryTodo
								.entrySet()) {
							Subquery sq = se.getKey();
							/*
							 * this subquery will be rescheduled, but to which
							 * node? this depends on the scheduler, no? for now,
							 * randomize
							 */
							NeverlandNode newNode = randomNodeFromSchedule(
									schedule, Arrays.asList(se.getValue()));
							
							// TODO: actually reschedule, see above for how

							// how do we avoid re-re-re-scheduling?
							// FIXME
						}
					}
					try {
						Thread.sleep(Constants.RESCHEDULER_SLEEP_MS);
					} catch (InterruptedException e) {
						// don't care
					}
					// repeat until we have all enough result sets
				} while (resultSets.size() < subqueries);
			}
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
					log.info("Opening new connection pool to "
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
					cpds.setNumHelperThreads(10);

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
					+ ", " + schedule.size() + " Subqueries");
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
