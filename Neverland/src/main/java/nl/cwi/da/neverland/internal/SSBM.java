package nl.cwi.da.neverland.internal;

import java.util.TreeMap;

public class SSBM {
	public static final String Q01 = "SELECT SUM(lo_extendedprice*lo_discount) AS revenue FROM lineorder, dwdate WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount between 1 AND 3 AND lo_quantity < 25;";
	public static final String Q02 = "SELECT SUM(lo_extendedprice*lo_discount) AS revenue FROM lineorder, dwdate WHERE lo_orderdate = d_datekey AND d_yearmonthnum = 199401 AND lo_discount between 4 AND 6 AND lo_quantity between 26 AND 35;";
	public static final String Q03 = "SELECT SUM(lo_extendedprice*lo_discount) AS revenue FROM lineorder, dwdate WHERE lo_orderdate = d_datekey AND d_weeknuminyear = 6 AND d_year = 1994  AND lo_discount between 5 AND 7and lo_quantity between 36 AND 40;";
	public static final String Q04 = "SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1 FROM lineorder, dwdate, part, supplier WHERE lo_orderdate = d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_category = 'MFGR#12' AND s_region = 'AMERICA' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1;";
	public static final String Q05 = "SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1 FROM lineorder, dwdate, part, supplier WHERE lo_orderdate = d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_brand1 between 'MFGR#2221' AND 'MFGR#2228' AND s_region = 'ASIA' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1;";
	public static final String Q06 = "SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1 FROM lineorder, dwdate, part, supplier WHERE lo_orderdate = d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_brand1 = 'MFGR#2221' AND s_region = 'EUROPE' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1;";
	public static final String Q07 = "SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, dwdate  WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_region = 'ASIA' AND s_region = 'ASIA' AND d_year >= 1992 AND d_year <= 1997 GROUP BY c_nation, s_nation, d_year ORDER BY d_year asc, revenue DESC;";
	public static final String Q08 = "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, dwdate WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_nation = 'UNITED STATES' AND s_nation = 'UNITED STATES' AND d_year >= 1992 AND d_year <= 1997 GROUP BY c_city, s_city, d_year ORDER BY d_year asc, revenue DESC;";
	public static final String Q09 = "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, dwdate WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_nation = 'UNITED KINGDOM' AND (c_city='UNITED KI1' OR c_city='UNITED KI5') AND (s_city='UNITED KI1' OR s_city='UNITED KI5') AND s_nation = 'UNITED KINGDOM' AND d_year >= 1992 AND d_year <= 1997 GROUP BY c_city, s_city, d_year ORDER BY d_year asc, revenue DESC;";
	public static final String Q10 = "SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue FROM customer, lineorder, supplier, dwdate WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_orderdate = d_datekey AND c_nation = 'UNITED KINGDOM' AND (c_city='UNITED KI1' OR c_city='UNITED KI5') AND (s_city='UNITED KI1' OR s_city='UNITED KI5') AND s_nation = 'UNITED KINGDOM' AND d_yearmonth = 'Dec1997' GROUP BY c_city, s_city, d_year ORDER BY d_year asc, revenue DESC;";
	public static final String Q11 = "SELECT d_year, c_nation, SUM(lo_revenue-lo_supplycost) AS profit1 FROM dwdate, customer, supplier, part, lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND lo_orderdate = d_datekey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') GROUP BY d_year, c_nation ORDER BY d_year, c_nation;";
	public static final String Q12 = "SELECT d_year, s_nation, p_category, SUM(lo_revenue-lo_supplycost) AS profit1 FROM dwdate, customer, supplier, part, lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND lo_orderdate = d_datekey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (d_year = 1997 OR d_year = 1998) AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') GROUP BY d_year, s_nation, p_category ORDER BY d_year, s_nation, p_category;";
	public static final String Q13 = "SELECT d_year, s_city, p_brand1, SUM(lo_revenue-lo_supplycost) AS profit1 FROM dwdate, customer, supplier, part, lineorder WHERE lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND lo_orderdate = d_datekey AND c_region = 'AMERICA' AND s_nation = 'UNITED STATES' AND (d_year = 1997 OR d_year = 1998) AND p_category = 'MFGR#14' GROUP BY d_year, s_city, p_brand1 ORDER BY d_year, s_city, p_brand1;";

	public static TreeMap<String, String> QUERIES = new TreeMap<String, String>();
	static {
		QUERIES.put("Q01", Q01);
		QUERIES.put("Q02", Q02);
		QUERIES.put("Q03", Q03);
		QUERIES.put("Q04", Q04);
		QUERIES.put("Q05", Q05);
		QUERIES.put("Q06", Q06);
		QUERIES.put("Q07", Q07);
		QUERIES.put("Q08", Q08);
		QUERIES.put("Q09", Q09);
		QUERIES.put("Q10", Q10);
		QUERIES.put("Q11", Q11);
		QUERIES.put("Q12", Q12);
		QUERIES.put("Q13", Q13);
	}
}