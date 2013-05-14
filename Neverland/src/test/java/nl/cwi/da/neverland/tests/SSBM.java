package nl.cwi.da.neverland.tests;

import java.util.HashMap;
import java.util.Map;

public class SSBM {
	public static final String Q01 = "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where lo_orderdate = d_datekey and d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;";
	public static final String Q02 = "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where lo_orderdate = d_datekey and d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35;";
	public static final String Q03 = "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994  and lo_discount between 5 and 7and lo_quantity between 36 and 40;";
	public static final String Q04 = "select sum(lo_revenue) as revenue, d_year, p_brand1 from lineorder, dwdate, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA' group by d_year, p_brand1 order by d_year, p_brand1;";
	public static final String Q05 = "select sum(lo_revenue) as revenue, d_year, p_brand1 from lineorder, dwdate, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand1 between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand1 order by d_year, p_brand1;";
	public static final String Q06 = "select sum(lo_revenue) as revenue, d_year, p_brand1 from lineorder, dwdate, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand1 = 'MFGR#2221' and s_region = 'EUROPE' group by d_year, p_brand1 order by d_year, p_brand1;";
	public static final String Q07 = "select c_nation, s_nation, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate  where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year >= 1992 and d_year <= 1997 group by c_nation, s_nation, d_year order by d_year asc, revenue desc;";
	public static final String Q08 = "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, revenue desc;";
	public static final String Q09 = "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_nation = 'UNITED KINGDOM' and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and s_nation = 'UNITED KINGDOM' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, revenue desc;";
	public static final String Q10 = "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_nation = 'UNITED KINGDOM' and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and s_nation = 'UNITED KINGDOM' and d_yearmonth = 'Dec1997' group by c_city, s_city, d_year order by d_year asc, revenue desc;";
	public static final String Q11 = "select d_year, c_nation, sum(lo_revenue-lo_supplycost) as profit1 from dwdate, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, c_nation order by d_year, c_nation;";
	public static final String Q12 = "select d_year, s_nation, p_category, sum(lo_revenue-lo_supplycost) as profit1 from dwdate, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and (d_year = 1997 or d_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, s_nation, p_category order by d_year, s_nation, p_category;";
	public static final String Q13 = "select d_year, s_city, p_brand1, sum(lo_revenue-lo_supplycost) as profit1 from dwdate, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (d_year = 1997 or d_year = 1998) and p_category = 'MFGR#14' group by d_year, s_city, p_brand1 order by d_year, s_city, p_brand1;";

	public static Map<String, String> QUERIES = new HashMap<String, String>();
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