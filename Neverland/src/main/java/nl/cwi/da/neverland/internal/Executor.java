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

import nl.cwi.da.neverland.internal.Scheduler.SubquerySchedule;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class Executor {

	public abstract List<ResultSet> executeSchedule(
			Scheduler.SubquerySchedule schedule) throws NeverlandException;

	public static class MultiThreadedExecutor extends Executor {
		private Map<String, ComboPooledDataSource> dataSources = new HashMap<String, ComboPooledDataSource>();
		private static Logger log = Logger
				.getLogger(MultiThreadedExecutor.class);

		private int connectionsPerNode;
		private ExecutorService executorService;

		public MultiThreadedExecutor(int executorThreads, int connectionsPerNode) {
			this.connectionsPerNode = connectionsPerNode;
			this.executorService = Executors
					.newFixedThreadPool(executorThreads);
		}

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
		public List<ResultSet> executeSchedule(SubquerySchedule schedule)
				throws NeverlandException {
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
					cpds.setMaxPoolSize(connectionsPerNode);
					// TODO: investigate here...
					cpds.setNumHelperThreads(10);

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
									InternalResultSet crs = null;
									Connection c = null;
									Statement s = null;
									ResultSet rs = null;

									try {
										log.debug("Running " + sq + " on "
												+ cpds.getJdbcUrl());
										c = cpds.getConnection();
										s = c.createStatement();
										rs = s.executeQuery(sq.getSql());

										crs = new InternalResultSet(rs);

										crs.beforeFirst();
										
										log.info("Got result on " + sq
												+ " from " + cpds.getJdbcUrl());
									} catch (SQLException e) {
										log.warn(e);
										e.printStackTrace();
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
							}, schedule.getTimeoutMs()));
				}
			}

			for (Future<ResultSet> rf : resultSetsFutures) {
				try {
					ResultSet r = rf.get();
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
			return resultSets;
		}
	}
}
