package nl.cwi.da.neverland.internal;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.rowset.CachedRowSet;

import nl.cwi.da.neverland.daemons.Coordinator;
import nl.cwi.da.neverland.internal.Scheduler.SubquerySchedule;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sun.rowset.CachedRowSetImpl;

public abstract class Executor {

	public abstract void executeSchedule(Scheduler.SubquerySchedule schedule,
			IoSession session) throws NeverlandException;

	public static class StupidExecutor extends Executor {
		private Map<String, ComboPooledDataSource> dataSources = new HashMap<String, ComboPooledDataSource>();
		private static Logger log = Logger.getLogger(StupidExecutor.class);

		private static final int EXECUTOR_THREADS = 1000;

		ExecutorService executorService = Executors
				.newFixedThreadPool(EXECUTOR_THREADS);
		ScheduledExecutorService canceller = Executors
				.newSingleThreadScheduledExecutor();

		private <T> Future<T> executeTask(Callable<T> c, long timeoutMS) {
			final Future<T> future = executorService.submit(c);
			canceller.schedule(new Callable<Void>() {
				public Void call() {
					future.cancel(true);
					return null;
				}
			}, timeoutMS, TimeUnit.MILLISECONDS);
			return future;
		}

		@Override
		public void executeSchedule(SubquerySchedule schedule, IoSession session)
				throws NeverlandException {
			// TODO do this in the background, possibly with the actor model!

			int subqueries = 0;

			List<Future<ResultSet>> resultSetsFutures = new ArrayList<Future<ResultSet>>();
			List<ResultSet> resultSets = new ArrayList<ResultSet>();

			for (Entry<NeverlandNode, List<Subquery>> sentry : schedule
					.entrySet()) {
				NeverlandNode nn = sentry.getKey();

				if (!dataSources.containsKey(nn.getId())) {
					ComboPooledDataSource cpds = new ComboPooledDataSource();
					try {
						cpds.setDriverClass(Constants.JDBC_DRIVER);
					} catch (PropertyVetoException e) {
						log.warn("Unable to load JDBC driver", e);
					}

					// JDBC login config, as advertised by worker
					cpds.setJdbcUrl(nn.getJdbcUrl());
					cpds.setUser(nn.getJdbcUser());
					cpds.setPassword(nn.getJdbcPass());

					// some config, rather arbitrary. however, number cpus?
					cpds.setMinPoolSize(0);
					cpds.setAcquireIncrement(1);
					cpds.setMaxPoolSize(8);

					dataSources.put(nn.getId(), cpds);
				}

				final ComboPooledDataSource cpds = dataSources.get(nn.getId());

				// right, so now execute all the queries
				for (final Subquery sq : sentry.getValue()) {
					subqueries++;

					resultSetsFutures.add(executeTask(
							new Callable<ResultSet>() {
								@Override
								public ResultSet call() throws Exception {
									CachedRowSet crs = new CachedRowSetImpl();
									Connection c = null;
									Statement s = null;
									ResultSet rs = null;

									try {
										log.info("Running " + sq + " on "
												+ cpds.getJdbcUrl());
										c = cpds.getConnection();
										s = c.createStatement();
										rs = s.executeQuery(sq.getSql());

										crs.populate(rs);

										log.info("Got result on " + sq
												+ " from " + cpds.getJdbcUrl());
									} catch (SQLException e) {
										log.warn(e);
									} finally {
										try {
											rs.close();
										} catch (SQLException se) {
										}
										try {
											s.close();
										} catch (SQLException se) {
										}
										try {
											c.close();
										} catch (SQLException se) {
										}
									}
									return crs;
								}
							}, schedule.getTimeoutMs()));
				}
			}

			for (Future<ResultSet> rf : resultSetsFutures) {
				try {
					resultSets.add(rf.get());
				} catch (Exception e) {
					log.warn(e);
				}
			}

			if (resultSets.size() != subqueries) {
				throw new NeverlandException(
						"Not enough result sets found to combine, something must have gone wrong.");
			}
			ResultCombiner rc = new ResultCombiner.ConcatResultCombiner();
			ResultSet aggrSet = rc.combine(schedule.getQuery(), resultSets);
			Coordinator.serializeResultSet(aggrSet, session);
			session.close(false);
		}
	}
}
