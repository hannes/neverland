package nl.cwi.da.neverland.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.NeverlandNode;
import nl.cwi.da.neverland.internal.Query;
import nl.cwi.da.neverland.internal.Rewriter;
import nl.cwi.da.neverland.internal.Rewriter.NotSoStupidRewriter;
import nl.cwi.da.neverland.internal.Scheduler;
import nl.cwi.da.neverland.internal.Scheduler.SubquerySchedule;
import nl.cwi.da.neverland.internal.Subquery;

import org.apache.log4j.Logger;
import org.junit.Test;

public class SchedulerTest {
	private static Logger log = Logger.getLogger(SchedulerTest.class);

	@Test
	public void StickySchedulerTest() throws NeverlandException {
		Rewriter rw = new NotSoStupidRewriter("lineitem", "l_orderkey", 0,
				60000);
		Query q = new Query(
				"SELECT SUM(l_extendedprice * l_discount) FROM lineitem");
		List<Subquery> sqs = rw.rewrite(q, 20);
		Scheduler s = new Scheduler.StickyScheduler();
		List<NeverlandNode> nn = makeNodeList(10);
		SubquerySchedule ss = s.schedule(nn, sqs);
		for (Entry<NeverlandNode, List<Subquery>> e : ss.entrySet()) {
			String info = e.getKey().getId() + " <= ";
			for (Subquery sq : e.getValue()) {
				info += sq.getSlice() + ", ";
			}
			log.info(info);
		}
	}

	private List<NeverlandNode> makeNodeList(int size) {
		List<NeverlandNode> l = new ArrayList<NeverlandNode>(size);
		for (int i = 0; i < size; i++) {
			l.add(new NeverlandNode("", "", "", Integer.toString(i)));
		}
		return l;
	}
}
