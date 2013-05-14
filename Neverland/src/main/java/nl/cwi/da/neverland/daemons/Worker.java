package nl.cwi.da.neverland.daemons;

import java.io.IOException;
import java.util.Iterator;

import nl.cwi.da.neverland.internal.Constants;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class Worker extends Thread implements Watcher {

	protected ZooKeeper zkc;

	private static Logger log = Logger.getLogger(Worker.class);

	private Constants.WorkerState workerState = Constants.WorkerState.initializing;

	private String jdbcUri;
	private String zookeeper;

	public Worker(String zooKeeper, String jdbcUri, String jdbcUser,
			String jdbcPass) {
		this.zookeeper = zooKeeper;
		this.jdbcUri = jdbcUri;
		// TODO: use and advertise JDBC credentials
	}

	@Override
	public void run() {
		try {
			zkc = new ZooKeeper(zookeeper, 2000, this);
		} catch (IOException e) {
			log.warn(e);
		}

		log.info("Neverland worker daemon starting. Advertising JDBC URI " + jdbcUri + " ...");
		while (workerState == Constants.WorkerState.initializing) {
			try {
				String thisNodeKey = Constants.ZK_PREFIX + "/"
						+ zkc.getSessionId();

				zkc.create(thisNodeKey, jdbcUri.getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

				log.info("Successfully advertised at " + thisNodeKey);
				workerState = Constants.WorkerState.normal;
			} catch (Exception e) {
				log.warn("Zookeeper not ready... retrying in "
						+ Constants.ADVERTISE_DELAY_MS + " ms");
				log.debug(e);
			}

			// TODO: check JDBC connection(?)
			// problem: we might not have the proper JDBC driver here...

			try {
				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
			} catch (InterruptedException e) {
				// ignore this.
			}
		}

		while (workerState == Constants.WorkerState.normal) {
			try {
				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
				// TODO: what to do here except keeping alive?
				// Answer: re-advertise? Check ZK docs!
			} catch (InterruptedException e) {
				//
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO: do we need this at all?
	}

	public static void main(String[] args) throws JSAPException {

		JSAP jsap = new JSAP();

		jsap.registerParameter(new FlaggedOption("jdbcuri").setShortFlag('j')
				.setLongFlag("jdbc-uri").setStringParser(JSAP.STRING_PARSER)
				.setRequired(true).setHelp("Database JDBC URI"));

		jsap.registerParameter(new FlaggedOption("jdbcuser").setShortFlag('u')
				.setLongFlag("jdbc-user").setStringParser(JSAP.STRING_PARSER)
				.setRequired(false).setDefault("")
				.setHelp("Database JDBC User"));

		jsap.registerParameter(new FlaggedOption("jdbcpass").setShortFlag('p')
				.setLongFlag("jdbc-pass").setStringParser(JSAP.STRING_PARSER)
				.setRequired(false).setDefault("")
				.setHelp("Database JDBC Password"));

		jsap.registerParameter(new FlaggedOption("zkhost").setShortFlag('z')
				.setLongFlag("zookeeper-hostname")
				.setStringParser(JSAP.INETADDRESS_PARSER).setRequired(false)
				.setDefault("localhost")
				.setHelp("Hostname of Zookeeper server to connect to"));

		jsap.registerParameter(new FlaggedOption("zkport").setShortFlag('k')
				.setLongFlag("zookeeper-port")
				.setStringParser(JSAP.INTEGER_PARSER).setRequired(false)
				.setDefault(Integer.toString(Constants.ZK_PORT))
				.setHelp("Portnumber of Zookeeper server to connect to"));

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

		new Worker(res.getInetAddress("zkhost").getHostAddress() + ":"
				+ res.getInt("zkport"), res.getString("jdbcuri"),
				res.getString("jdbcuser"), res.getString("jdbcpass")).start();
	}
}
