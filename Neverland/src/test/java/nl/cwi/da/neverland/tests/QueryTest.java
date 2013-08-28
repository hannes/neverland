package nl.cwi.da.neverland.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.Query;

import org.junit.Test;

public class QueryTest {

	@Test
	public void orderFullTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo ORDER BY b  LIMIT 10 OFFSET 10;");
		assertTrue(q.needsLimiting());
		assertTrue(q.needsSorting());
	}

	@Test
	public void orderLimitOffsetTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo LIMIT 10  OFFSET 10;");
		assertTrue(q.needsLimiting());
		assertFalse(q.needsSorting());
	}

	@Test
	public void orderLimitTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo ORDER BY b LIMIT 10;");
		assertTrue(q.needsLimiting());
		assertTrue(q.needsSorting());
	}
	
	@Test
	public void orderOffsetTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo ORDER BY b OFFSET 10;");
		assertTrue(q.needsLimiting());
		assertTrue(q.needsSorting());
	}
	
	
	@Test
	public void offsetTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo OFFSET 10;");
		assertTrue(q.needsLimiting());
		assertFalse(q.needsSorting());
	}
	
	
	@Test
	public void limitTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo LIMIT 10;");
		assertTrue(q.needsLimiting());
		assertFalse(q.needsSorting());
	}
	
	
	@Test
	public void orderTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo ORDER BY b;");
		assertFalse(q.needsLimiting());
		assertTrue(q.needsSorting());
	}


	@Test
	public void noneTest() throws NeverlandException {
		Query q = new Query("SELECT a FROM foo;");
		assertFalse(q.needsLimiting());
		assertFalse(q.needsSorting());
	}
}
