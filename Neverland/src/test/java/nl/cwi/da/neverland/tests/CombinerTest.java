package nl.cwi.da.neverland.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import nl.cwi.da.neverland.internal.Constants;
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
	public void ssbmIntegrationTest() throws NeverlandException, SQLException,
			InterruptedException {
		Rewriter rw = new NotSoStupidRewriter("lineorder", "lo_orderkey", 0,
				60000);

		for (Entry<String, String> e : SSBM.QUERIES.entrySet()) {
			System.out.println(e.getKey());
			Query q = new Query(e.getValue());
			Scheduler.SubquerySchedule schedule = new StupidScheduler()
					.schedule(Arrays.asList(new NeverlandNode(
							"jdbc:monetdb://localhost:50000/ssbm-sf1",
							"monetdb", "monetdb", "42",0)), rw.rewrite(q, 10));

			List<ResultSet> rss = new MultiThreadedExecutor(10, 2)
					.executeSchedule(schedule);
			ResultSet rc = r.combine(q, rss);
			ResultCombiner.printResultSet(rc, System.out);
			// assertTrue(rc.next());
		}
	}

	@Before
	public void cleanup() {
		rss.clear();
	}

	@SafeVarargs()
	private static <T> List<T> al(T... a) {
		return Arrays.asList(a);
	}

	private static List<Object> alo(Object... a) {
		return Arrays.asList(a);
	}

	// we like the performance so far...
	@Ignore
	@Test
	public void performanceTest() throws SQLException, NeverlandException {
		int datasize = 100000;
		int reps = 1000;
		int groupsize = 10;

		Integer[] values = new Integer[datasize];
		Integer[] groups = new Integer[datasize];

		Random rnd = new Random();
		for (int i = 0; i < datasize; i++) {
			values[i] = rnd.nextInt();
			groups[i] = rnd.nextInt(groupsize);
		}

		TestResultSet rs1 = new TestResultSet(al("max_a", "b"), al(
				Types.INTEGER, Types.INTEGER), al("INT", "INT"), datasize);
		rs1.setColumn(1, alo(values));
		rs1.setColumn(2, alo(groups));

		rss.add(rs1);

		Query q = new Query("select max(a),b from table1 group by b");

		for (int rp = 0; rp < reps; rp++) {
			System.out.println(rp);
			ResultSet rs = r.combine(q, rss);
			int size = 0;
			while (rs.next()) {
				size++;
			}
			assertEquals(size, groupsize);
			rs1.beforeFirst();
		}

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
	public void order() throws NeverlandException, SQLException {
		Query q = new Query(
				"select sum(a),b,c from table1 group by b,c order by  b, c desc");
		TestResultSet rs1 = new TestResultSet(al("sum_a", "b", "c"), al(
				Types.INTEGER, Types.INTEGER, Types.INTEGER), al("INT", "INT",
				"INT"), 3);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5, 2, 3));
		rs1.setColumn(2, alo(1, 1, 2));
		rs1.setColumn(3, alo(1, 2, 2));

		rs2.setColumn(1, alo(2, 3, 4));
		rs2.setColumn(2, alo(1, 2, 3));
		rs2.setColumn(3, alo(1, 2, 1));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);

		rs.next();
		assertEquals(rs.getInt(1), 2);
		assertEquals(rs.getInt(2), 1);
		assertEquals(rs.getInt(3), 2);
		rs.next();
		assertEquals(rs.getInt(1), 7);
		assertEquals(rs.getInt(2), 1);
		assertEquals(rs.getInt(3), 1);
		rs.next();
		assertEquals(rs.getInt(1), 6);
		assertEquals(rs.getInt(2), 2);
		assertEquals(rs.getInt(3), 2);
		rs.next();
		assertEquals(rs.getInt(1), 4);
		assertEquals(rs.getInt(2), 3);
		assertEquals(rs.getInt(3), 1);

		assertFalse(rs.next());
	}

	@Test
	public void varcharGroupTest() throws NeverlandException, SQLException {
		Query q = new Query("select sum(a),b from table1 group by b");

		TestResultSet rs1 = new TestResultSet(al("sum_a", "b"), al(
				Types.INTEGER, Types.VARCHAR), al("INT", "VARCHAR"), 3);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5, 2, 3));
		rs1.setColumn(2, alo("g1", "g2", "g3"));

		rs2.setColumn(1, alo(1, 1, 3));
		rs2.setColumn(2, alo("g2", "g3", "g4"));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);
		int size = 0;
		while (rs.next()) {
			if (rs.getString(2).equals("g1")) {
				assertEquals(rs.getInt(1), 5);
			}
			if (rs.getString(2).equals("g2")) {
				assertEquals(rs.getInt(1), 3);
			}
			if (rs.getString(2).equals("g3")) {
				assertEquals(rs.getInt(1), 4);
			}
			if (rs.getString(2).equals("g4")) {
				assertEquals(rs.getInt(1), 3);
			}
			size++;
		}
		assertEquals(size, 4);
	}

	@Test
	public void groupVarProjectMultiGroupCol() throws NeverlandException,
			SQLException {
		Query q = new Query("select sum(a),b,c from table1 group by b,c");
		TestResultSet rs1 = new TestResultSet(al("sum_a", "b", "c"), al(
				Types.INTEGER, Types.INTEGER, Types.INTEGER), al("INT", "INT",
				"INT"), 3);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5, 2, 3));
		rs1.setColumn(2, alo(1, 1, 2));
		rs1.setColumn(3, alo(1, 2, 2));

		rs2.setColumn(1, alo(2, 3, 4));
		rs2.setColumn(2, alo(1, 2, 3));
		rs2.setColumn(3, alo(1, 2, 1));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			if (rs.getInt(2) == 1 && rs.getInt(3) == 1) {
				assertEquals(rs.getInt(1), 7);
			}
			if (rs.getInt(2) == 1 && rs.getInt(3) == 2) {
				assertEquals(rs.getInt(1), 2);
			}
			if (rs.getInt(2) == 2 && rs.getInt(3) == 2) {
				assertEquals(rs.getInt(1), 6);
			}
			if (rs.getInt(2) == 3 && rs.getInt(3) == 1) {
				assertEquals(rs.getInt(1), 4);
			}

			size++;
		}
		assertEquals(size, 4);
	}

	@Test
	public void sumTest() throws NeverlandException, SQLException {
		Query q = new Query("select sum(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("sum_a"), al(Types.INTEGER),
				al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 11);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void minTest() throws NeverlandException, SQLException {
		Query q = new Query("select min(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("min_a"), al(Types.INTEGER),
				al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));
		rs3.setColumn(1, alo(2));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 2);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void maxTest() throws NeverlandException, SQLException {
		Query q = new Query("select max(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("max_a"), al(Types.INTEGER),
				al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));
		rs3.setColumn(1, alo(2));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 6);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void avgTest() throws NeverlandException, SQLException {
		Query q = new Query("select avg(a) from table1");

		TestResultSet rs1 = new TestResultSet(
				al("avg_a", Constants.GROUP_NAME), al(Types.DOUBLE,
						Types.INTEGER), al("DOUBLE", "INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5.4));
		rs1.setColumn(2, alo(2));

		rs2.setColumn(1, alo(6.23));
		rs2.setColumn(2, alo(1));

		rs3.setColumn(1, alo(2.5));
		rs3.setColumn(2, alo(10));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getDouble(1), 3.23, 0.01);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void countExprTest() throws NeverlandException, SQLException {
		Query q = new Query("select count(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("count_star"),
				al(Types.INTEGER), al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));
		rs3.setColumn(1, alo(2));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 13);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void countStarTest() throws NeverlandException, SQLException {
		Query q = new Query("select count(*) from table1");

		TestResultSet rs1 = new TestResultSet(al("count_star"),
				al(Types.INTEGER), al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);
		TestResultSet rs3 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));
		rs3.setColumn(1, alo(2));

		rss.add(rs1);
		rss.add(rs2);
		rss.add(rs3);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 13);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void integerTest() throws NeverlandException, SQLException {
		Query q = new Query("select max(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("count_star"),
				al(Types.INTEGER), al("INT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5));
		rs2.setColumn(1, alo(6));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getInt(1), 6);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void floatTest() throws NeverlandException, SQLException {
		Query q = new Query("select max(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("count_star"),
				al(Types.FLOAT), al("FLOAT"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5.1));
		rs2.setColumn(1, alo(6.4));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getFloat(1), 6.4, 0.001);
			size++;
		}
		assertEquals(size, 1);
	}

	@Test
	public void doubleTest() throws NeverlandException, SQLException {
		Query q = new Query("select max(a) from table1");

		TestResultSet rs1 = new TestResultSet(al("count_star"),
				al(Types.DOUBLE), al("DOUBLE"), 1);

		TestResultSet rs2 = new TestResultSet(rs1);

		rs1.setColumn(1, alo(5.1D));
		rs2.setColumn(1, alo(6.4D));

		rss.add(rs1);
		rss.add(rs2);

		ResultSet rs = r.combine(q, rss);

		int size = 0;
		while (rs.next()) {
			assertEquals(rs.getDouble(1), 6.4, 0.001);
			size++;
		}
		assertEquals(size, 1);
	}
}
