package nl.cwi.da.neverland.daemons;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import nl.cwi.da.neverland.internal.Constants;
import nl.cwi.da.neverland.internal.NeverlandNode;

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
import com.mchange.v2.ser.SerializableUtils;

public class Worker extends Thread implements Watcher {

	protected ZooKeeper zkc;

	private static Logger log = Logger.getLogger(Worker.class);

	private Constants.WorkerState workerState = Constants.WorkerState.initializing;

	private String jdbcUri;
	private String zookeeper;

	private String jdbcUser;
	private String jdbcPass;

	public Worker(String zooKeeper, String jdbcUri, String jdbcUser,
			String jdbcPass) {
		this.zookeeper = zooKeeper;
		this.jdbcUri = jdbcUri;
		this.jdbcUser = jdbcUser;
		this.jdbcPass = jdbcPass;
	}

	@Override
	public void run() {
		try {
			zkc = new ZooKeeper(zookeeper, 2000, this);
		} catch (IOException e) {
			log.warn(e);
		}

		log.info("Neverland worker daemon starting. Advertising JDBC URI "
				+ jdbcUri + " ...");

		NeverlandNode thisNode = new NeverlandNode("localhost", 0, jdbcUri,
				jdbcUser, jdbcPass, 0);

		try {
			thisNode.setHostname(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e1) {
			log.warn("Unable to get hostname");
		}

		String thisNodeKey = "";
		while (workerState == Constants.WorkerState.initializing) {
			try {
				Class.forName(Constants.JDBC_DRIVER);
				Connection c = DriverManager.getConnection(jdbcUri, jdbcUser,
						jdbcPass);
				Statement s = c.createStatement();
				ResultSet rs = s.executeQuery("SELECT 1");
				if (!rs.next()) {
					throw new SQLException("Wadde hadde dudde da?");
				}
				rs.close();
				s.close();
				c.close();
				thisNodeKey = Constants.ZK_PREFIX + "/" + zkc.getSessionId();
				
				zkc.create(thisNodeKey,
						SerializableUtils.toByteArray(thisNode),
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

				log.info("Successfully advertised at " + thisNodeKey);
				workerState = Constants.WorkerState.normal;

			} catch (Exception e) {
				log.warn("Unable to advertise DB at " + jdbcUri + " to ZK at "
						+ zookeeper, e);
			}

			try {
				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
			} catch (InterruptedException e) {
				// ignore this.
			}
		}

		final OperatingSystemMXBean myOsBean = ManagementFactory
				.getOperatingSystemMXBean();

		while (workerState == Constants.WorkerState.normal) {
			thisNode.setLoad(myOsBean.getSystemLoadAverage());
			thisNode.setId(zkc.getSessionId());
			try {

				zkc.setData(thisNodeKey,
						SerializableUtils.toByteArray(thisNode), -1);

			} catch (Exception e) {
				log.warn("ZK Error", e);
				// TODO: go to initialization...
			}

			try {
				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
			} catch (InterruptedException e) {
				// ignore this.
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// not needed atm
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
