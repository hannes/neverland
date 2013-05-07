package nl.cwi.da.neverland.internal;

import java.util.HashMap;
import java.util.List;

public abstract class Scheduler {

	public static class SubquerySchedule extends
			HashMap<NeverlandNode, List<Subquery>> {
		private static final long serialVersionUID = 1L;
		private Query q;

		public SubquerySchedule(Query q) {
			this.q = q;
		}

		public Query getQuery() {
			return q;
		}

		public long getTimeoutMs() {
			return 60*1000;
		}
	}

	public abstract SubquerySchedule schedule(List<NeverlandNode> nodes,
			List<Subquery> subqueries) throws NeverlandException;

	public static class StupidScheduler extends Scheduler {
		@Override
		public SubquerySchedule schedule(List<NeverlandNode> nodes,
				List<Subquery> subqueries) throws NeverlandException {

			if (nodes.size() < 1) {
				throw new NeverlandException(
						"Need at least one node to schedule queries to");
			}

			if (subqueries.size() < 1) {
				throw new NeverlandException(
						"Need at least one subquery to schedule");
			}

			NeverlandNode n1 = nodes.get(0);
			SubquerySchedule schedule = new SubquerySchedule(subqueries.get(0)
					.getParent());
			schedule.put(n1, subqueries);
			return schedule;
		}
	}
}
