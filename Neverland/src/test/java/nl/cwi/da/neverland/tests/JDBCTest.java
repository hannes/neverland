package nl.cwi.da.neverland.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map.Entry;

import nl.cwi.da.neverland.client.NeverlandResultSet;
import nl.cwi.da.neverland.daemons.Coordinator;
import nl.cwi.da.neverland.daemons.Worker;
import nl.cwi.da.neverland.internal.BDB;
import nl.cwi.da.neverland.internal.ResultCombiner;
import nl.cwi.da.neverland.internal.SSBM;

import org.junit.Test;

import com.martiansoftware.jsap.JSAPException;

public class JDBCTest {

	@Test
	public void testConnectionSSBM() throws InterruptedException,
			ClassNotFoundException, SQLException {

		// bring up coordinator
		(new Thread() {
			public void run() {
				try {
					Coordinator.main(new String[0]);
				} catch (JSAPException e) {
					e.printStackTrace();
				}
			}
		}).start();

		Thread.sleep(1000);

		// bring up 2 workers
		(new Thread() {
			public void run() {
				try {
					Worker.main("-d nl.cwi.monetdb.jdbc.MonetDriver -j jdbc:monetdb://localhost:50000/ssbm-sf1 -u monetdb -p monetdb"
							.split(" "));
				} catch (JSAPException e) {
					e.printStackTrace();
				}
			}
		}).start();

		(new Thread() {
			public void run() {
				try {
					Worker.main("-d nl.cwi.monetdb.jdbc.MonetDriver -j jdbc:monetdb://localhost:50000/ssbm-sf1 -u monetdb -p monetdb"
							.split(" "));
				} catch (JSAPException e) {
					e.printStackTrace();
				}
			}
		}).start();

		Thread.sleep(5000);

		Class.forName("nl.cwi.da.neverland.client.NeverlandDriver");
		Connection conn = DriverManager
				.getConnection("jdbc:neverland://localhost:50002/db");

		Statement s = conn.createStatement();
		while (true) {
			for (Entry<String, String> e : SSBM.QUERIES.entrySet()) {
				ResultSet rs = s.executeQuery(e.getValue());
				// ResultCombiner.printResultSet(rs, System.out);
			}
			Thread.sleep(100);

		}

	}

	@Test
	public void testConnectionBDB() throws InterruptedException,
			ClassNotFoundException, SQLException {

		// bring up coordinator
		(new Thread() {
			public void run() {
				try {
					Coordinator.main("-r uservisits.id[1:10000] -d 1000"
							.split(" "));
				} catch (JSAPException e) {
					e.printStackTrace();
				}
			}
		}).start();

		Thread.sleep(1000);

		// bring up 2 workers
		(new Thread() {
			public void run() {
				try {
					Worker.main("-d nl.cwi.monetdb.jdbc.MonetDriver -j jdbc:monetdb://localhost:50000/bdb-small -u monetdb -p monetdb"
							.split(" "));
				} catch (JSAPException e) {
					e.printStackTrace();
				}
			}
		}).start();

		(new Thread() {
			public void run() {
				try {
					Worker.main("-d nl.cwi.monetdb.jdbc.MonetDriver -j jdbc:monetdb://localhost:50000/bdb-small -u monetdb -p monetdb"
							.split(" "));
				} catch (JSAPException e) {
					e.printStackTrace();
				}
			}
		}).start();

		Thread.sleep(5000);

		Class.forName("nl.cwi.da.neverland.client.NeverlandDriver");
		Connection conn = DriverManager
				.getConnection("jdbc:neverland://localhost:50002/db");

		Statement s = conn.createStatement();
		while (true) {
			ResultSet rs = s.executeQuery(BDB.Q03a);
			ResultCombiner.printResultSet(rs, System.out);
			Thread.sleep(100);

		}

	}

	@Test
	public void testResultSet() throws SQLException {
		Statement s = mock(Statement.class);
		ResultSet rs = new NeverlandResultSet(s,
				"l_orderkey\tl_extendedprice\tl_returnflag\n"
						+ "INTEGER\tDECIMAL\tVARCHAR(100)\n"
						+ "42\t42.2\t\"asdf\"\n" + "21\t21.1\t\"fdsa\"\n");

		rs.next();
		assertEquals(rs.getInt(1), 42);
		assertEquals(rs.getInt("l_orderkey"), 42);
		assertEquals(rs.getDouble(2), 42.2, 0.001);
		assertEquals(rs.getDouble("l_extendedprice"), 42.2, 0.001);
		assertEquals(rs.getString("l_returnflag"), "asdf");

		// check indices that are out of bounds
		try {
			rs.getInt(0);
			fail();
		} catch (Exception e) {
		}
		try {
			rs.getInt(-111);
			fail();
		} catch (Exception e) {
		}

		try {
			rs.getInt(4);
			fail();
		} catch (Exception e) {
		}

		// test non-existing names
		try {
			rs.getInt("foobar");
			fail();
		} catch (Exception e) {
		}

		// test result set metadata
		ResultSetMetaData rsm = rs.getMetaData();
		assertEquals(rsm.getColumnName(1), "l_orderkey");
		assertEquals(rsm.getColumnTypeName(1), "integer");
		assertEquals(rsm.getColumnType(1), 4);

		// test iteration
		rs.beforeFirst();
		int count = 0;
		while (rs.next()) {
			assertTrue(rs.getInt(1) == 42 || rs.getInt(1) == 21);
			count++;
		}
		assertEquals(count, 2);

		rs.close();
	}
}
