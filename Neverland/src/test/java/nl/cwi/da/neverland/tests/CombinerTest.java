package nl.cwi.da.neverland.tests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import nl.cwi.da.neverland.internal.Executor.MultiThreadedExecutor;
import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.NeverlandNode;
import nl.cwi.da.neverland.internal.Query;
import nl.cwi.da.neverland.internal.ResultCombiner;
import nl.cwi.da.neverland.internal.Rewriter;
import nl.cwi.da.neverland.internal.Rewriter.NotSoStupidRewriter;
import nl.cwi.da.neverland.internal.Scheduler;
import nl.cwi.da.neverland.internal.Scheduler.StupidScheduler;

import org.junit.Test;

public class CombinerTest {

	@Test
	public void firstTest() throws NeverlandException, SQLException {

		Rewriter rw = new NotSoStupidRewriter("lineorder", "lo_orderkey", 0,
				60000, 10);
		Query q = new Query(SSBM.Q04);
		Scheduler.SubquerySchedule schedule = new StupidScheduler().schedule(
				Arrays.asList(new NeverlandNode(
						"jdbc:monetdb://localhost:50000/ssbm-sf1", "monetdb",
						"monetdb", "42")), rw.rewrite(q));

		List<ResultSet> rss = new MultiThreadedExecutor(100, 10)
				.executeSchedule(schedule);

		ResultCombiner r = new ResultCombiner.SmartResultCombiner();
		ResultSet rc = r.combine(q, rss);
		ResultCombiner.printResultSet(rc, System.out);
	}
}
