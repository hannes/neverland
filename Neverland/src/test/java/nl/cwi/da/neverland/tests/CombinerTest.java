package nl.cwi.da.neverland.tests;

import static org.mockito.Mockito.mock;

import java.util.Arrays;

import nl.cwi.da.neverland.internal.Executor.StupidExecutor;
import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.NeverlandNode;
import nl.cwi.da.neverland.internal.Query;
import nl.cwi.da.neverland.internal.Rewriter;
import nl.cwi.da.neverland.internal.Rewriter.NotSoStupidRewriter;
import nl.cwi.da.neverland.internal.Scheduler;
import nl.cwi.da.neverland.internal.Scheduler.StupidScheduler;

import org.apache.mina.core.session.IoSession;
import org.junit.Test;

public class CombinerTest {

	@Test
	public void firstTest() throws NeverlandException {
		Rewriter rw = new NotSoStupidRewriter("lineorder", "lo_orderkey", 0,
				60000, 99);

		Scheduler.SubquerySchedule schedule = new StupidScheduler().schedule(
				Arrays.asList(new NeverlandNode(
						"jdbc:monetdb://localhost:50000/ssbm-sf1", "monetdb",
						"monetdb", "42")), rw.rewrite(new Query(SSBM.Q01)));

		IoSession session = mock(IoSession.class);
		new StupidExecutor().executeSchedule(schedule, session);

		
	}
}
