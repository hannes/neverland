package nl.cwi.da.neverland.internal;

import java.util.TreeMap;

public class BDB {
	public static final String Q01a = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000;";
	public static final String Q01b = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 100;";
	public static final String Q01c = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 10;";

	public static final String Q02a = "SELECT sourcePrefix,SUM(adRevenue) FROM (SELECT SUBSTRING(sourceIP, 1, 8) AS sourcePrefix,adRevenue FROM uservisits) as ST GROUP BY sourcePrefix;";
	public static final String Q02b = "SELECT sourcePrefix,SUM(adRevenue) FROM (SELECT SUBSTRING(sourceIP, 1, 10) AS sourcePrefix,adRevenue FROM uservisits) as ST GROUP BY sourcePrefix;";
	public static final String Q02c = "SELECT sourcePrefix,SUM(adRevenue) FROM (SELECT SUBSTRING(sourceIP, 1, 12) AS sourcePrefix,adRevenue FROM uservisits) as ST GROUP BY sourcePrefix;";

	public static final String Q03a = "SELECT sourceIP, sum(adRevenue) AS totalRevenue, avg(pageRank) AS pageRank FROM rankings as R JOIN uservisits AS UV  ON R.pageURL = UV.destURL WHERE UV.visitDate BETWEEN  '1980-01-01' AND  '1980-04-01' GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1;";
	public static final String Q03b = "SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank FROM rankings AS R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits AS UV WHERE UV.visitDate > '1980-01-01' AND UV.visitDate < '1983-01-01') AS  NUV ON (R.pageURL = NUV.destURL) GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1;";
	public static final String Q03c = "SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank FROM rankings AS R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits AS UV WHERE UV.visitDate > '1980-01-01' AND UV.visitDate < '2010-01-01') AS  NUV ON (R.pageURL = NUV.destURL) GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1;";

	public static TreeMap<String, String> QUERIESA = new TreeMap<String, String>();
	static {
		QUERIESA.put("Q01a", Q01a);
		QUERIESA.put("Q02a", Q02a);
		QUERIESA.put("Q03a", Q03a);
	}
}