package nl.cwi.da.neverland;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

public class Coordinator extends Thread implements Watcher {
	protected ZooKeeper zkc;

	private static Logger log = Logger.getLogger(Coordinator.class);

	private String zookeeper;
	private int jdbcPort;

	public Coordinator(String zooKeeper, int jdbcPort) {
		this.zookeeper = zooKeeper;
		this.jdbcPort = jdbcPort;

		try {
			Class.forName(Constants.JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			log.fatal("JDBC driver not found on classpath", e);
		}
	}

	private Constants.CoordinatorState coordinatorState = Constants.CoordinatorState.initializing;

	@Override
	public void run() {
		try {
			zkc = new ZooKeeper(zookeeper, 2000, this);
		} catch (IOException e) {
			log.warn(e);
		}

		while (coordinatorState == Constants.CoordinatorState.initializing) {
			try {
				// create tree root if not there yet
				if (zkc.exists(Constants.ZK_PREFIX, this) == null) {
					zkc.create(Constants.ZK_PREFIX, null, Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}

				log.info("Successfully created root node at at "
						+ Constants.ZK_PREFIX);
				coordinatorState = Constants.CoordinatorState.normal;
			} catch (Exception e) {
				log.warn("Zookeeper not ready... retrying in "
						+ Constants.ADVERTISE_DELAY_MS + " ms", e);
			}

			try {
				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
			} catch (InterruptedException e) {
				// ignore this.
			}
		}

		// create socket handler using NIO/MINA
		IoAcceptor acceptor = new NioSocketAcceptor();
		// acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast(
				"codec",
				new ProtocolCodecFilter(new TextLineCodecFactory(Charset
						.forName("UTF-8"))));

		acceptor.setHandler(new JdbcSocketHandler());
		acceptor.getSessionConfig().setReadBufferSize(2048);
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
		try {
			acceptor.bind(new InetSocketAddress(jdbcPort));
		} catch (IOException e1) {
			log.warn("Unable to open JDBC server at port " + jdbcPort, e1);
		}

		while (coordinatorState == Constants.CoordinatorState.normal) {
			try {

				List<String> nodes = zkc.getChildren(Constants.ZK_PREFIX, this);
				log.info("Found " + nodes.size() + " advertised nodes in "
						+ Constants.ZK_PREFIX);
				for (String n : nodes) {
					log.info(n
							+ "="
							+ new String(zkc.getData(Constants.ZK_PREFIX + "/"
									+ n, false, null)));
				}
			} catch (Exception e) {
				log.warn("Zookeeper not ready... retrying in "
						+ Constants.POLL_DELAY_MS + " ms", e);
			}

			try {
				Thread.sleep(Constants.POLL_DELAY_MS);
			} catch (InterruptedException e) {
				// ignore this.
			}
		}

	}

	@Override
	public void process(WatchedEvent event) {
		// TODO: do we need this at all?
	}

	public static void main(String[] args) {
		startInternalZookeeperServer();
		// TODO: use args for zookeeper setup and jdbc port
		new Coordinator("localhost:" + Constants.ZK_PORT, Constants.JDBC_PORT)
				.start();
	}

	public static void startInternalZookeeperServer() {
		final ServerConfig config = new ServerConfig();
		try {
			config.parse(new String[] { Integer.toString(Constants.ZK_PORT),
					createTempDirectory().getPath() });
		} catch (IOException e1) {
			log.warn(e1);
		}

		(new Thread() {
			public void run() {
				ZooKeeperServerMain zks = new ZooKeeperServerMain();
				try {
					zks.runFromConfig(config);
				} catch (IOException e) {
					log.fatal(e);
					System.exit(-1);
				}
			}
		}).start();
	}

	private static class JdbcSocketHandler extends IoHandlerAdapter {
		@Override
		public void messageReceived(IoSession session, Object message)
				throws Exception {
			String str = message.toString();
			if (!str.startsWith(Constants.MAGIC_HEADER)) {
				log.warn("No magic header in message, discarding.");
				session.write("Use proper header");
				session.close(true);
				return;
			}
			str = str.substring(Constants.MAGIC_HEADER.length());
			if ("PING".equals(str.toUpperCase())) {
				session.write("PONG");
			}
			if (str.toUpperCase().startsWith("EXEC")) {
				String sql = str.substring(5);
				log.info("QQ: " + sql);
				// some dummy data
				session.write("l_orderkey\tl_extendedprice\tl_returnflag\n"
						+ "INTEGER\tDECIMAL\tVARCHAR(100)\n"
						+ "42\t42.2\t\"asdf\"\n");
			}
		}
	}

	protected void handle(String line) {
		log.info(line);
	}

	public static File createTempDirectory() throws IOException {
		final File temp;

		temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

		if (!(temp.delete())) {
			throw new IOException("Could not delete temp file: "
					+ temp.getAbsolutePath());
		}

		if (!(temp.mkdir())) {
			throw new IOException("Could not create temp directory: "
					+ temp.getAbsolutePath());
		}

		return (temp);
	}
}
