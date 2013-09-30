package nl.cwi.da.neverland.daemons;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.UUID;

import nl.cwi.da.neverland.internal.Constants;
import nl.cwi.da.neverland.internal.NeverlandNode;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.mchange.v2.ser.SerializableUtils;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.Group.Membership;
import com.twitter.common.zookeeper.ZooKeeperClient;

public class Worker extends Thread implements Watcher {

	private static Logger log = Logger.getLogger(Worker.class);

	private String jdbcUri;
	private String jdbcDriver;
	private InetSocketAddress zookeeper;

	private String jdbcUser;
	private String jdbcPass;

	private String uuid;

	public Worker(InetSocketAddress zooKeeper, String jdbcDriver,
			String jdbcUri, String jdbcUser, String jdbcPass) {
		this.zookeeper = zooKeeper;
		this.jdbcDriver = jdbcDriver;
		this.jdbcUri = jdbcUri;
		this.jdbcUser = jdbcUser;
		this.jdbcPass = jdbcPass;
		this.uuid = UUID.randomUUID().toString();
	}

	private ZooKeeperClient tzkc;
	private Group zkg;

	private static class BS implements Supplier<byte[]> {
		private byte[] data;

		public BS() {
		}

		public BS set(byte[] data) {
			this.data = data;
			return this;
		}

		@Override
		public byte[] get() {
			return data;
		}
	}

	@Override
	public void run() {

		log.info("Neverland worker daemon starting. Advertising JDBC URI "
				+ jdbcUri + " ...");

		final NeverlandNode thisNode = new NeverlandNode("localhost", uuid,
				jdbcDriver, jdbcUri, jdbcUser, jdbcPass, 0);

		try {
			thisNode.setHostname(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e1) {
			log.warn("Unable to get hostname");
		}

		final OperatingSystemMXBean myOsBean = ManagementFactory
				.getOperatingSystemMXBean();

		tzkc = new ZooKeeperClient(Amount.of(Constants.ZK_TIMEOUT_MS,
				Time.MILLISECONDS), zookeeper);
		zkg = new Group(tzkc, Ids.OPEN_ACL_UNSAFE, Constants.ZK_PREFIX);
		BS memberDataSupplier = new BS();
		Membership groupMembership;

		// verify the JDBC db is online
		try {
			Class.forName(jdbcDriver);
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
			memberDataSupplier.set(SerializableUtils.toByteArray(thisNode));
			groupMembership = zkg.join(memberDataSupplier);
			groupMembership.updateMemberData();
			log.info("Successfully verified connection to " + jdbcUri);
		} catch (Exception e) {
			log.error("Unable to verify DB at " + jdbcUri, e);
			return;
		}

		while (true) {
			try {
				thisNode.setLoad(myOsBean.getSystemLoadAverage());
				thisNode.setId(uuid);
				memberDataSupplier.set(SerializableUtils.toByteArray(thisNode));
				groupMembership.updateMemberData();

				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
			} catch (Exception e) {
				log.warn(e);
			}

		}

	}

	@Override
	public void process(WatchedEvent event) {
		// not needed atm
	}

	public static void main(String[] args) throws JSAPException {
		JSAP jsap = new JSAP();

		jsap.registerParameter(new FlaggedOption("jdbcdriver")
				.setShortFlag('d').setLongFlag("jdbc-driver")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setHelp("Database JDBC driver class name"));

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

		new Worker(new InetSocketAddress(res.getInetAddress("zkhost")
				.getHostAddress(), res.getInt("zkport")),
				res.getString("jdbcdriver"), res.getString("jdbcuri"),
				res.getString("jdbcuser"), res.getString("jdbcpass")).start();
	}
}
