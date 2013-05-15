package nl.cwi.da.neverland.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

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
			// TODO : fixme
			return 3600 * 1000;
		}

		public void schedule(NeverlandNode n, Subquery q) {
			if (!containsKey(n)) {
				put(n, new ArrayList<Subquery>());
			}
			get(n).add(q);
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

	public static class RoundRobinScheduler extends Scheduler {

		private int lastNodeIndex = 0;

		@Override
		public SubquerySchedule schedule(List<NeverlandNode> nodes,
				List<Subquery> subqueries) throws NeverlandException {
			SubquerySchedule schedule = new SubquerySchedule(subqueries.get(0)
					.getParent());

			for (Subquery sq : subqueries) {
				lastNodeIndex = (lastNodeIndex + 1) % nodes.size();
				schedule.schedule(nodes.get(lastNodeIndex), sq);
			}
			return schedule;
		}

	}

	public static class LoadBalancingScheduler extends Scheduler {

		private Random rnd = new Random();

		@Override
		public SubquerySchedule schedule(List<NeverlandNode> nodes,
				List<Subquery> subqueries) throws NeverlandException {
			SubquerySchedule schedule = new SubquerySchedule(subqueries.get(0)
					.getParent());

			for (Subquery sq : subqueries) {
				NeverlandNode n1 = nodes.get(rnd.nextInt(nodes.size()));
				NeverlandNode n2 = nodes.get(rnd.nextInt(nodes.size()));
				if (n1.getLoad() < n2.getLoad()) {
					schedule.schedule(n1, sq);
				} else {
					schedule.schedule(n2, sq);
				}
			}
			return schedule;
		}

	}

	public static class RandomScheduler extends Scheduler {

		private Random rnd = new Random();

		@Override
		public SubquerySchedule schedule(List<NeverlandNode> nodes,
				List<Subquery> subqueries) throws NeverlandException {
			SubquerySchedule schedule = new SubquerySchedule(subqueries.get(0)
					.getParent());

			for (Subquery sq : subqueries) {
				NeverlandNode n1 = nodes.get(rnd.nextInt(nodes.size()));
				schedule.schedule(n1, sq);
			}
			return schedule;
		}
	}

	public static class StickyScheduler extends Scheduler {

		@Override
		public SubquerySchedule schedule(List<NeverlandNode> nodes,
				List<Subquery> subqueries) throws NeverlandException {
			SubquerySchedule schedule = new SubquerySchedule(subqueries.get(0)
					.getParent());
			for (Subquery sq : subqueries) {
				int node = sq.getSlice() % nodes.size();
				schedule.schedule(nodes.get(node), sq);
			}
			return schedule;
		}

	}
}
