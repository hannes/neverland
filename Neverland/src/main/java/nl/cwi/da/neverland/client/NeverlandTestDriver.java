package nl.cwi.da.neverland.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

	private static class ThreadResult {
		private Map<String, StatisticalDescription> timings;
		private double totalDuration;
		private long queriesRun;
		private double qps;
	}

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

		System.out.println(NeverlandTestDriver.class.getSimpleName() + " "
				+ threads + " thread(s)");

		List<Future<ThreadResult>> resultStats = new ArrayList<Future<ThreadResult>>();

		ExecutorService executorService = Executors.newFixedThreadPool(res
				.getInt("threads"));

		for (int i = 0; i < threads; i++) {
			resultStats.add(executorService
					.submit(new Callable<ThreadResult>() {
						@Override
						public ThreadResult call() throws Exception {
							Map<String, StatisticalDescription> timings = new HashMap<String, StatisticalDescription>();

							Connection c = DriverManager.getConnection(jdbc);
							Statement s = c.createStatement();
							for (int i = 0; i < warmup; i++) {
								for (Entry<String, String> e : SSBM.QUERIES
										.entrySet()) {
									double time = executeQuery(e.getValue(), s);
									System.out.println(e.getKey() + ": " + time);
								}
							}
							long start = System.currentTimeMillis();

							for (int i = 0; i < runs; i++) {
								System.out.println("Running test set "
										+ (i + 1) + " of " + runs);
								for (Entry<String, String> e : SSBM.QUERIES
										.entrySet()) {
									if (!timings.containsKey(e.getKey())) {
										timings.put(e.getKey(),
												new StatisticalDescription());
									}
									double time = executeQuery(e.getValue(), s);
									timings.get(e.getKey()).addValue(time);
									System.out.println(e.getKey() + ": " + time);
								}
							}

							s.close();
							c.close();

							double durationSecs = (System.currentTimeMillis() - start) / 1000.0;

							ThreadResult tr = new ThreadResult();
							tr.queriesRun = runs * SSBM.QUERIES.size();
							tr.totalDuration = durationSecs;
							tr.timings = timings;
							tr.qps = tr.queriesRun / tr.totalDuration;
							return tr;
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
		for (Future<ThreadResult> rf : resultStats) {
			try {
				ThreadResult d = rf.get();
				System.out.println(d.qps);
				// TODO: aggregate query timings

			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}

	}
}
