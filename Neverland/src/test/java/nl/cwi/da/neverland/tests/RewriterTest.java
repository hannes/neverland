package nl.cwi.da.neverland.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map.Entry;

import nl.cwi.da.neverland.internal.BDB;
import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.NeverlandNode;
import nl.cwi.da.neverland.internal.Query;
import nl.cwi.da.neverland.internal.Rewriter;
import nl.cwi.da.neverland.internal.Rewriter.NotSoStupidRewriter;
import nl.cwi.da.neverland.internal.SSBM;
import nl.cwi.da.neverland.internal.Subquery;

import org.apache.log4j.Logger;
import org.junit.Test;

public class RewriterTest {

	private static Logger log = Logger.getLogger(RewriterTest.class);
	private Rewriter rw = new NotSoStupidRewriter("lineitem", "l_orderkey", 0,
			60000, 10);

	@Test
	public void selectStarRewriterTest() throws NeverlandException {

		Query q = new Query(
				"SELECT SUM(l_extendedprice * l_discount) FROM lineitem");
		List<Subquery> sqs = rw.rewrite(q, 6);

		for (Subquery sq : sqs) {
			log.info(sq);
			assertTrue(sq.getSql().startsWith(q.getSql()));
		}

	}

	@Test
	public void limitOrderRewriterTest() throws NeverlandException {
		Query q = new Query(
				"SELECT SUM(l_extendedprice * l_discount) FROM lineitem ORDER BY foo LIMIT 100;");
		List<Subquery> sqs = rw.rewrite(q, 6);
		for (Subquery sq : sqs) {
			log.info(sq);
		}
	}

	@Test
	public void existingRestrictionRewriterTest() throws NeverlandException {
		Query q = new Query(
				"SELECT SUM(l_extendedprice * l_discount) FROM lineitem where l_extendedprice > 42;");
		List<Subquery> sqs = rw.rewrite(q, 6);
		for (Subquery sq : sqs) {
			log.info(sq);
		}

	}

	@Test
	public void ssbmRewriterTest() throws NeverlandException {
		Rewriter rw = new NotSoStupidRewriter("lineorder", "lo_orderkey", 0,
				600000000, 1000);
		for (Entry<String, String> e : SSBM.QUERIES.entrySet()) {
			log.info(e.getKey());
			if (!e.getKey().equals("Q01"))
				continue;
			Query q = new Query(e.getValue());
			List<Subquery> sqs = rw.rewrite(q, 6);
			for (Subquery sq : sqs) {
				log.info(sq);
			}
		}
	}

	@Test
	public void bdbRewriterTest() throws NeverlandException {
		Rewriter rw = new NotSoStupidRewriter("uservisits", "id", 0, 10000, 10);
		Query q = new Query(BDB.Q03a);
		List<Subquery> sqs = rw.rewrite(q, 10);
		assertEquals(10, sqs.size());
		for (Subquery sq : sqs) {
			log.info(sq);
		}
	}

	@Test
	public void constructRewriterTest() throws NeverlandException {
		Rewriter rw = Rewriter.constructRewriterFromDb(new NeverlandNode(
				"localhost", "42", "nl.cwi.monetdb.jdbc.MonetDriver",
				"jdbc:monetdb://localhost:50000/ssbm-sf1", "monetdb",
				"monetdb", 1), 1000);
		Query q = new Query(SSBM.Q01);
		List<Subquery> sqs = rw.rewrite(q, 100);
		for (Subquery sq : sqs) {
			log.info(sq);
		}
	}

	@Test
	public void constructPostgresTest() throws NeverlandException {
		Rewriter rw = Rewriter.constructRewriterFromDb(
				new NeverlandNode("localhost", "42", "org.postgresql.Driver",
						"jdbc:postgresql://localhost:5432/ssbm-sf1", "ssbm",
						"ssbm", 1), 1000);
		Query q = new Query(SSBM.Q01);
		List<Subquery> sqs = rw.rewrite(q, 10000000);
		for (Subquery sq : sqs) {
			// log.info(sq);
		}
	}
}
