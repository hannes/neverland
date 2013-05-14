package nl.cwi.da.neverland.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map.Entry;

import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.Query;
import nl.cwi.da.neverland.internal.Rewriter;
import nl.cwi.da.neverland.internal.Rewriter.NotSoStupidRewriter;
import nl.cwi.da.neverland.internal.Subquery;

import org.apache.log4j.Logger;
import org.junit.Test;

public class RewriterTest {

	private static Logger log = Logger.getLogger(RewriterTest.class);
	private Rewriter rw = new NotSoStupidRewriter("lineitem", "l_orderkey", 0,
			60000);

	@Test
	public void selectStarRewriterTest() throws NeverlandException {

		Query q = new Query(
				"SELECT SUM(l_extendedprice * l_discount) FROM lineitem");
		List<Subquery> sqs = rw.rewrite(q, 6);

		for (Subquery sq : sqs) {
			log.info(sq);
			assertEquals(q, sq.getParent());
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
			assertEquals(q, sq.getParent());
		}
	}

	@Test
	public void unionRewriterTest() {

	}

	@Test
	public void existingRestrictionRewriterTest() throws NeverlandException {
		Query q = new Query(
				"SELECT SUM(l_extendedprice * l_discount) FROM lineitem where l_extendedprice > 42;");
		List<Subquery> sqs = rw.rewrite(q, 6);
		for (Subquery sq : sqs) {
			log.info(sq);
			assertEquals(q, sq.getParent());
		}

	}

	@Test
	public void ssbmRewriterTest() throws NeverlandException {
		Rewriter rw = new NotSoStupidRewriter("lineorder", "lo_orderkey", 0,
				60000);
		for (Entry<String, String> e : SSBM.QUERIES.entrySet()) {
			log.info(e.getKey());
			Query q = new Query(e.getValue());
			List<Subquery> sqs = rw.rewrite(q, 6);
			for (Subquery sq : sqs) {
				log.info(sq);
			}
		}
	}
}
