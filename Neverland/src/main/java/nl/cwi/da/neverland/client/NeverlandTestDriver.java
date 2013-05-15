package nl.cwi.da.neverland.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import nl.cwi.da.neverland.internal.Constants;
import nl.cwi.da.neverland.internal.SSBM;
import nl.cwi.da.neverland.internal.StatisticalDescription;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class NeverlandTestDriver {
	public static void main(String[] args) throws JSAPException {

		JSAP jsap = new JSAP();

		jsap.registerParameter(new FlaggedOption("host").setShortFlag('h')
				.setLongFlag("host").setStringParser(JSAP.STRING_PARSER)
				.setRequired(false).setDefault("localhost")
				.setHelp("Hostname of the Neverland Coordinator"));

		jsap.registerParameter(new FlaggedOption("port").setShortFlag('p')
				.setLongFlag("port").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false)
				.setDefault(Integer.toString(Constants.JDBC_PORT))
				.setHelp("JDBC port on the Neverland Coordinator"));

		jsap.registerParameter(new FlaggedOption("threads").setShortFlag('t')
				.setLongFlag("threads").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false).setDefault("1")
				.setHelp("Number of threads"));

		jsap.registerParameter(new FlaggedOption("warmup").setShortFlag('w')
				.setLongFlag("warmup").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false).setDefault("0")
				.setHelp("Number of warmup runs through the query set"));

		jsap.registerParameter(new FlaggedOption("runs").setShortFlag('r')
				.setLongFlag("runs").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false).setDefault("1")
				.setHelp("Number of test runs through the query set"));

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

		final String jdbc = "jdbc:neverland://" + res.getString("host") + ":"
				+ res.getInt("port") + "/db";
		final int warmup = res.getInt("warmup");
		final int runs = res.getInt("runs");
		final int threads = res.getInt("threads");

		
		System.out.println(NeverlandTestDriver.class.getSimpleName() + " " + threads + " thread(s)");

		List<Future<StatisticalDescription>> resultStats = new ArrayList<Future<StatisticalDescription>>();

		ExecutorService executorService = Executors.newFixedThreadPool(res
				.getInt("threads"));

		for (int i = 0; i < threads; i++) {
			resultStats.add(executorService
					.submit(new Callable<StatisticalDescription>() {
						@Override
						public StatisticalDescription call() throws Exception {
							Connection c = DriverManager.getConnection(jdbc);
							Statement s = c.createStatement();
							StatisticalDescription d = new StatisticalDescription();
							for (int i = 0; i < warmup; i++) {
								for (Entry<String, String> e : SSBM.QUERIES
										.entrySet()) {
									executeQuery(e.getValue(), s);
								}
							}
							for (int i = 0; i < runs; i++) {
								for (Entry<String, String> e : SSBM.QUERIES
										.entrySet()) {
									d.addValue(executeQuery(e.getValue(), s));
								}
							}

							s.close();
							c.close();
							d.calculate();
							return d;
						}

						private double executeQuery(String query, Statement s)
								throws SQLException {
							long start = System.currentTimeMillis();
							s.executeQuery(query);
							return (System.currentTimeMillis() - start) / 1000.0;
						}
					}));

		}

		StatisticalDescription results = new StatisticalDescription();
		executorService.shutdown();
		for (Future<StatisticalDescription> rf : resultStats) {
			try {
				StatisticalDescription d = rf.get();
				results.merge(d);

			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}
		
		
		System.out.println(results.sum);
		System.out.println((runs * SSBM.QUERIES.size()));
		System.out.println( (runs * SSBM.QUERIES.size())/results.sum);

	}
}
