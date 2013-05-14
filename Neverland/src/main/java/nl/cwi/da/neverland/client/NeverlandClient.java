package nl.cwi.da.neverland.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nl.cwi.da.neverland.internal.Constants;
import nl.cwi.da.neverland.internal.ResultCombiner;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;

public class NeverlandClient {
	public static void main(String[] args) throws JSAPException {

		JSAP jsap = new JSAP();
		jsap.registerParameter(new FlaggedOption("query").setShortFlag('s')
				.setShortFlag('q').setLongFlag("query")
				.setStringParser(JSAP.STRING_PARSER).setRequired(false)
				.setHelp("A single SQL query"));

		jsap.registerParameter(new FlaggedOption("host").setShortFlag('h')
				.setLongFlag("host").setStringParser(JSAP.STRING_PARSER)
				.setRequired(false).setDefault("localhost")
				.setHelp("Hostname of the Neverland Coordinator"));

		jsap.registerParameter(new FlaggedOption("port").setShortFlag('p')
				.setLongFlag("port").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false)
				.setDefault(Integer.toString(Constants.JDBC_PORT))
				.setHelp("Hostname of the Neverland Coordinator"));

		jsap.registerParameter(new FlaggedOption("timeout").setShortFlag('t')
				.setLongFlag("timeout").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false).setDefault("60")
				.setHelp("Timeout in Seconds (ignored atm)"));

		jsap.registerParameter(new UnflaggedOption("files")
				.setStringParser(JSAP.INTEGER_PARSER).setRequired(false)
				.setGreedy(true).setHelp("Files with SQL statements"));

		JSAPResult res = jsap.parse(args);

		if (!res.success()) {
			@SuppressWarnings("rawtypes")
			Iterator errs = res.getErrorMessageIterator();
			while (errs.hasNext()) {
				System.err.println(errs.next());
			}

			System.err.println("Usage: " + jsap.getUsage() + "\nParameters: "
					+ jsap.getHelp());
			System.exit(-1);
		}

		try {
			Class.forName("nl.cwi.da.neverland.client.NeverlandDriver");
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}

		try {
			Connection conn = DriverManager.getConnection("jdbc:neverland://"
					+ res.getString("host") + ":" + res.getInt("port") + "/db");
			Statement s = conn.createStatement();

			List<String> queries = new ArrayList<String>();
			if (res.userSpecified("query")) {
				queries.add(res.getString("query"));
			}
			if (res.userSpecified("files")) {
				queries.add(res.getString("query"));
			}
			if (queries.size() == 0) {
				System.err.println("No queries specified");
			}

			for (String q : queries) {
				System.out.println(q);
				ResultSet rs = s.executeQuery(q);
				ResultCombiner.printResultSet(rs, System.out);
				rs.close();
			}
			s.close();
			conn.close();
		} catch (SQLException e) {
			System.err.println("Failed to run query: " + e.getMessage());
			System.exit(-1);
		}

	}
}
