package nl.cwi.da.neverland.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Scheduler {
	public abstract Map<NeverlandNode, List<Subquery>> schedule(
			List<NeverlandNode> nodes, List<Subquery> subqueries)
			throws NeverlandException;

	public class StupidScheduler extends Scheduler {
		@Override
		public Map<NeverlandNode, List<Subquery>> schedule(
				List<NeverlandNode> nodes, List<Subquery> subqueries)
				throws NeverlandException {

			if (nodes.size() < 1) {
				throw new NeverlandException(
						"Need at least one node to schedule queries to");
			}

			if (subqueries.size() < 1) {
				throw new NeverlandException(
						"Need at least one subquery to schedule");
			}

			NeverlandNode n1 = nodes.get(0);
			Map<NeverlandNode, List<Subquery>> retMap = new HashMap<NeverlandNode, List<Subquery>>();
			retMap.put(n1, subqueries);
			return retMap;
		}
	}
}
