package nl.cwi.da.neverland.internal;

import java.beans.PropertyVetoException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nl.cwi.da.neverland.daemons.Coordinator;
import nl.cwi.da.neverland.internal.Scheduler.SubquerySchedule;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public abstract class Executor {

	public abstract void executeSchedule(Scheduler.SubquerySchedule schedule,
			IoSession session) throws NeverlandException;

	public static class StupidExecutor extends Executor {
		private Map<String, ComboPooledDataSource> dataSources = new HashMap<String, ComboPooledDataSource>();
		private static Logger log = Logger.getLogger(StupidExecutor.class);

		@Override
		public void executeSchedule(SubquerySchedule schedule, IoSession session)
				throws NeverlandException {
			// TODO do this in the background, possibly with the actor model!

			List<ResultSet> resultSets = new ArrayList<ResultSet>();
			int subqueries = 0;

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
					
					cpds.setJdbcUrl(nn.getJdbc());
					log.info(nn.getJdbc());
					cpds.setUser("monetdb"); // TODO: add this to Zookeeper config
					cpds.setPassword("monetdb");

					// the settings below are optional -- c3p0 can work with
					// defaults
					cpds.setMinPoolSize(0);
					cpds.setAcquireIncrement(1);
					cpds.setMaxPoolSize(8);

					dataSources.put(nn.getId(), cpds);
				}

				ComboPooledDataSource cpds = dataSources.get(nn.getId());

				// right, so now execute all the queries and
				for (Subquery sq : sentry.getValue()) {
					subqueries++;
					try {
						ResultSet rs = cpds.getConnection().createStatement()
								.executeQuery(sq.getSql());
						resultSets.add(rs);
					} catch (SQLException e) {
						log.warn(e);
					}
				}
			}

			if (resultSets.size() != subqueries) {
				throw new NeverlandException(
						"Not enough result sets found to combine, a something must have gone wrong.");
			}
			ResultCombiner rc = new ResultCombiner.ConcatResultCombiner();
			ResultSet aggrSet = rc.combine(schedule.getQuery(), resultSets);
			Coordinator.serializeResultSet(aggrSet, session);
			session.close(false);
		}
	}
}
