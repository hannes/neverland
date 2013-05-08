package nl.cwi.da.neverland.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CombinerTest {
	ResultCombiner r = new ResultCombiner.SmartResultCombiner();
	List<ResultSet> rss = new ArrayList<ResultSet>();

	@Ignore
	@Test
	public void outputObserveTest() throws NeverlandException, SQLException {

		Rewriter rw = new NotSoStupidRewriter("lineorder", "lo_orderkey", 0,
				60000, 10);
		Query q = new Query(SSBM.Q04);
		Scheduler.SubquerySchedule schedule = new StupidScheduler().schedule(
				Arrays.asList(new NeverlandNode(
						"jdbc:monetdb://localhost:50000/ssbm-sf1", "monetdb",
						"monetdb", "42")), rw.rewrite(q));

		List<ResultSet> rss = new MultiThreadedExecutor(100, 10)
				.executeSchedule(schedule);

		ResultSet rc = r.combine(q, rss);
		ResultCombiner.printResultSet(rc, System.out);
	}

	@Before
	public void cleanup() {
		rss.clear();
	}

	@SafeVarargs
	private static <T> List<T> al(T... a) {
		return Arrays.asList(a);
	}

	private static List<Object> alo(Object... a) {
		return Arrays.asList(a);
	}

	@Test
	public void noGroupVarProjectionSingleGroupCol() throws NeverlandException,
			SQLException {
		Query q = new Query("select sum(a) from table1 group by b");

		TestResultSet rs1 = new TestResultSet(al("sum_a"), al(Types.INTEGER),
				al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);
		TestResultSet rs4 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));
		rs3.setColumn(1, alo(7));
		rs4.setColumn(1, alo(8));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);
		rss.add(rs4);

		ResultSet rs = r.combine(q, rss);
		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 26);
			size++;
		}
		assertEquals(size, 1);

	}

	@Test
	public void noGroupVarProjectionMultiGroupCol() throws NeverlandException,
			SQLException {
		Query q = new Query("select sum(a) from table1 group by b,c");

		TestResultSet rs1 = new TestResultSet(al("sum_a"), al(Types.INTEGER),
				al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);
		TestResultSet rs4 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));
		rs3.setColumn(1, alo(7));
		rs4.setColumn(1, alo(8));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);
		rss.add(rs4);

		ResultSet rs = r.combine(q, rss);
		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 26);
			size++;
		}
		assertEquals(size, 1);

	}

	@Test
	public void groupVarProjectSingleGroupCol() throws NeverlandException,
			SQLException {
		Query q = new Query("select sum(a),b from table1 group by b");

		TestResultSet rs1 = new TestResultSet(al("sum_a", "b"), al(
				Types.INTEGER, Types.INTEGER), al("INT", "INT"), 3);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5, 2, 3));
		rs1.setColumn(2, alo(1, 2, 3));

		rs2.setColumn(1, alo(1, 1, 3));
		rs2.setColumn(2, alo(2, 3, 4));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);
		int size = 0;
		while (rs.next()) {
			if (rs.getInt(2) == 1) {
				assertEquals(rs.getInt(1), 5);
			}
			if (rs.getInt(2) == 2) {
				assertEquals(rs.getInt(1), 3);
			}
			if (rs.getInt(2) == 3) {
				assertEquals(rs.getInt(1), 4);
			}
			if (rs.getInt(2) == 4) {
				assertEquals(rs.getInt(1), 3);
			}
			size++;
		}
		assertEquals(size, 4);
	}

	@Test
	public void groupVarProjectMultiGroupCol() throws NeverlandException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void sumTest() throws NeverlandException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void minTest() throws NeverlandException {
		Query q = new Query("select min(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void maxTest() throws NeverlandException {
		Query q = new Query("select max(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void avgTest() throws NeverlandException {
		Query q = new Query("select avg(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void countExprTest() throws NeverlandException {
		Query q = new Query("select count(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void countStarTest() throws NeverlandException {
		Query q = new Query("select count(*),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void integerTest() throws NeverlandException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void longTest() throws NeverlandException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void floatTest() throws NeverlandException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void doubleTest() throws NeverlandException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		fail();
	}

	@Test
	public void varcharTest() throws NeverlandException {
		Query q = new Query("select count(a),b,c from table1 group by b,c");
		fail();
	}
}
