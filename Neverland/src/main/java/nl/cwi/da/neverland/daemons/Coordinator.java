package nl.cwi.da.neverland.daemons;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.cwi.da.neverland.internal.Constants;
import nl.cwi.da.neverland.internal.Executor;
import nl.cwi.da.neverland.internal.NeverlandException;
import nl.cwi.da.neverland.internal.NeverlandNode;
import nl.cwi.da.neverland.internal.Query;
import nl.cwi.da.neverland.internal.ResultCombiner;
import nl.cwi.da.neverland.internal.Rewriter;
import nl.cwi.da.neverland.internal.Scheduler;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
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
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import com.google.gson.Gson;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.StringParser;
import com.mchange.v2.ser.SerializableUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ZooKeeperClient;

public class Coordinator extends Thread implements Watcher {

	private static Logger log = Logger.getLogger(Coordinator.class);

	private InetSocketAddress zookeeper;
	private int jdbcPort;

	private Executor executor;
	private NeverlandScenario scenario;

	private Rewriter rewriter;
	private Scheduler scheduler;
	private ResultCombiner combiner;

	private int httpPort;

	private long shardSize = 1;

	public static enum NeverlandScenario {
		baseline, loadbalance, rewriteround, rewriterandom, rewriteload, neverland, sticky;
	}

	public Coordinator(NeverlandScenario scenario, InetSocketAddress zooKeeper,
			int jdbcPort, int httpPort, long shardSize) {
		this.zookeeper = zooKeeper;
		this.jdbcPort = jdbcPort;
		this.httpPort = httpPort;
		this.executor = new Executor.MultiThreadedExecutor(100,10);
		this.scenario = scenario;
		this.shardSize = shardSize;
		this.rewriter = null;
	}

	public Coordinator(NeverlandScenario scenario, InetSocketAddress zooKeeper,
			int jdbcPort, int httpPort, long shardSize, Rewriter rewriter) {
		this(scenario, zooKeeper, jdbcPort, httpPort, shardSize);
		this.rewriter = rewriter;
	}

	private ZooKeeperClient tzkc;
	private Group zkg;

	@Override
	public void run() {
		log.info("Neverland coordinator daemon started with scenario "
				+ scenario + " and Zookeeper at " + zookeeper);

		startInternalWebserver(httpPort);

		// setup neverland according to scenario
		switch (scenario) {
		case baseline:
			// overwrite more clever rewriter
			this.rewriter = new Rewriter.StupidRewriter();
			this.scheduler = new Scheduler.RoundRobinScheduler();
			this.combiner = new ResultCombiner.PassthruCombiner();
			break;

		case loadbalance:
			// overwrite more clever rewriter
			this.rewriter = new Rewriter.StupidRewriter();
			this.scheduler = new Scheduler.LoadBalancingScheduler();
			this.combiner = new ResultCombiner.PassthruCombiner();
			break;

		case rewriteround:
			this.scheduler = new Scheduler.RoundRobinScheduler();
			this.combiner = new ResultCombiner.SmartResultCombiner();
			break;

		case rewriterandom:
			this.scheduler = new Scheduler.RandomScheduler();
			this.combiner = new ResultCombiner.SmartResultCombiner();
			break;

		case rewriteload:
			this.scheduler = new Scheduler.LoadBalancingScheduler();
			this.combiner = new ResultCombiner.SmartResultCombiner();
			break;

		case sticky:
			this.scheduler = new Scheduler.StickyScheduler();
			this.combiner = new ResultCombiner.SmartResultCombiner();
			break;

		case neverland:
			this.scheduler = new Scheduler.StickyLoadBalancingScheduler();
			this.combiner = new ResultCombiner.SmartResultCombiner();
			break;
		}
		this.scheduler = new Scheduler.StickyScheduler();

		tzkc = new ZooKeeperClient(Amount.of(Constants.ZK_TIMEOUT_MS,
				Time.MILLISECONDS), zookeeper);
		try {
			ZooKeeper zk = tzkc.get();
			if (zk.exists(Constants.ZK_PREFIX, false) == null) {
				zk.create(Constants.ZK_PREFIX, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
		} catch (Exception e2) {
			log.warn(e2);
		}

		zkg = new Group(tzkc, Ids.OPEN_ACL_UNSAFE, Constants.ZK_PREFIX);

		List<NeverlandNode> nodes;
		do {
			nodes = getCurrentNodes();
			if (nodes.size() > 0) {
				// now ask the first node to be alive for the table
				// structure
				try {
					if (this.rewriter == null) {
						this.rewriter = Rewriter.constructRewriterFromDb(
								nodes.get(0), shardSize);
						log.info("Using auto-generated rewriter: "
								+ this.rewriter);

					}
				} catch (NeverlandException e) {
					log.warn("Unable to initialize rewriter", e);
				}
			}

			try {
				Thread.sleep(Constants.ADVERTISE_DELAY_MS);
				log.info("Waiting for more nodes...");
			} catch (InterruptedException e) {
				// ignore this.
			}
		} while (nodes.size() < 1);

		// create socket handler using NIO/MINA
		IoAcceptor acceptor = new NioSocketAcceptor();
		// acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast(
				"codec",
				new ProtocolCodecFilter(new TextLineCodecFactory(Charset
						.forName("UTF-8"))));

		acceptor.setHandler(new JdbcSocketHandler(this));
		acceptor.getSessionConfig().setReadBufferSize(2048);
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
		try {
			acceptor.bind(new InetSocketAddress(jdbcPort));
			log.info("JDBC server ready at port " + jdbcPort);
		} catch (IOException e1) {
			log.warn("Unable to open JDBC server at port " + jdbcPort, e1);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO: do we need this at all?
		// maybe if we want to react if a node goes down

		// log.warn(event);
	}

	public static class NeverlandScenarioParser extends StringParser {

		@Override
		public Object parse(String arg0) throws ParseException {
			try {
				return NeverlandScenario.valueOf(arg0.trim().toLowerCase());
			} catch (IllegalArgumentException iae) {
				throw new ParseException(iae);
			}
		}
	}

	public static void main(String[] args) throws JSAPException {

		JSAP jsap = new JSAP();

		jsap.registerParameter(new FlaggedOption("zkhost").setShortFlag('z')
				.setLongFlag("zookeeper-hostname")
				.setStringParser(JSAP.INETADDRESS_PARSER).setRequired(false)
				.setDefault("localhost")
				.setHelp("Hostname of Zookeeper server to connect to"));

		jsap.registerParameter(new FlaggedOption("zkport").setShortFlag('p')
				.setLongFlag("zookeeper-port")
				.setStringParser(JSAP.INTEGER_PARSER).setRequired(false)
				.setDefault(Integer.toString(Constants.ZK_PORT))
				.setHelp("Portnumber of Zookeeper server to connect to"));

		jsap.registerParameter(new FlaggedOption("jdbcport").setShortFlag('j')
				.setLongFlag("jdbc-port").setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false)
				.setDefault(Integer.toString(Constants.JDBC_PORT))
				.setHelp("TCP port number for the JDBC server to listen on"));

		jsap.registerParameter(new FlaggedOption("httpport")
				.setShortFlag('m')
				.setLongFlag("http-port")
				.setStringParser(JSAP.INTEGER_PARSER)
				.setRequired(false)
				.setDefault(Integer.toString(Constants.MONITOR_PORT))
				.setHelp(
						"TCP port number for the HTTP monitoring server to listen on"));

		jsap.registerParameter(new FlaggedOption("shardsize").setShortFlag('d')
				.setLongFlag("shard-size").setStringParser(JSAP.LONG_PARSER)
				.setRequired(false)
				.setDefault(Long.toString(Constants.DEFAULT_SHARD_SIZE))
				.setHelp("Size of the fact table shards"));

		jsap.registerParameter(new FlaggedOption("hardcoderewrite")
				.setShortFlag('r')
				.setLongFlag("hardcode-rewrite")
				.setStringParser(JSAP.STRING_PARSER)
				.setRequired(false)
				.setHelp(
						"Hard-code table, column and key range to rwrite on. Format: table.col[colmin:colmax]"));

		jsap.registerParameter(new FlaggedOption("scenario").setShortFlag('s')
				.setLongFlag("neverland-scenario")
				.setStringParser(new NeverlandScenarioParser())
				.setRequired(false).setDefault("neverland")
				.setHelp("Neverland scenario to run (internal)"));

		jsap.registerParameter(new FlaggedOption("zkinternal")
				.setShortFlag('i')
				.setLongFlag("zookeeper-internal")
				.setStringParser(JSAP.BOOLEAN_PARSER)
				.setRequired(false)
				.setDefault("true")
				.setHelp(
						"Whether or not to start the built-in Zookeeper server"));

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

		NeverlandScenario scenario = (NeverlandScenario) res
				.getObject("scenario");

		if (res.getBoolean("zkinternal")) {
			startInternalZookeeperServer(res.getInt("zkport"));
			// sleep a bit to allow ZK server to bind to its port (less
			// exceptions)
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// don't care
			}
		}

		Rewriter rw = null;
		if (res.contains("hardcoderewrite")) {
			String rewritedef = res.getString("hardcoderewrite");
			Pattern rewritedefpattern = Pattern
					.compile("^\\s*(\\w+).(\\w+)\\[(\\d+):(\\d+)\\]\\s*$");
			Matcher rewritedefmatcher = rewritedefpattern.matcher(rewritedef);

			if (rewritedefmatcher.matches()) {
				rw = new Rewriter.NotSoStupidRewriter(
						rewritedefmatcher.group(1), rewritedefmatcher.group(2),
						Long.parseLong(rewritedefmatcher.group(3)),
						Long.parseLong(rewritedefmatcher.group(4)),
						res.getLong("shardsize"));
				log.info("Using hard-coded rewriter: " + rw);

			} else {
				log.warn("Rewrite hardcode given, but not used due to format");
			}
		}

		new Coordinator(scenario, new InetSocketAddress(res.getInetAddress(
				"zkhost").getHostAddress(), res.getInt("zkport")),
				res.getInt("jdbcport"), res.getInt("httpport"),
				res.getLong("shardsize"), rw).start();
	}

	public static void startInternalZookeeperServer(final int port) {
		final ServerConfig config = new ServerConfig();
		try {
			config.parse(new String[] { Integer.toString(port),
					createTempDirectory().getPath() });
		} catch (IOException e1) {
			log.warn(e1);
		}

		(new Thread() {
			public void run() {
				ZooKeeperServerMain zks = new ZooKeeperServerMain();
				try {
					zks.runFromConfig(config);
					log.info("Started internal Zookeeper server on port "
							+ port);
				} catch (IOException e) {
					log.fatal(e);
					System.exit(-1);
				}
			}
		}).start();
	}

	public static class DataServlet extends HttpServlet {

		private Coordinator coord;
		private Gson gson = new Gson();

		public DataServlet(Coordinator coord) {
			this.coord = coord;
		}

		private static final long serialVersionUID = 1L;

		protected void doGet(HttpServletRequest req, HttpServletResponse resp)
				throws ServletException, IOException {
			Map<String, Object> json = new HashMap<String, Object>();
			List<NeverlandNode> nn = coord.getCurrentNodes();
			json.put("nodes", nn);
			json.put("numNodes", nn.size());
			json.put("queries", coord.getLoggedQueries());

			resp.setContentType("application/json");
			resp.getWriter().write(gson.toJson(json));
		}
	}

	public static class JarResourceHandler extends ResourceHandler {
		private String resPrefix = "";
		private static Logger log = Logger.getLogger(JarResourceHandler.class);

		@Override
		public Resource getResource(String path) throws MalformedURLException {
			if (path == null || !path.startsWith("/")) {
				throw new MalformedURLException(path);
			}
			try {
				return Resource.newResource(Coordinator.class.getClassLoader()
						.getResource(resPrefix + path));
			} catch (IOException e) {
				log.warn(e);
				return null;
			}
		}

		@Override
		public void setResourceBase(String base) {
			resPrefix = base;
		}

	}

	public void startInternalWebserver(int port) {
		Server server = new Server();
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setPort(port);
		server.addConnector(connector);

		// static content.
		ResourceHandler staticResourceHandler = new JarResourceHandler();
		staticResourceHandler.setResourceBase("monitor");

		ContextHandler staticContextHandler = new ContextHandler();
		staticContextHandler.setContextPath("/");
		staticContextHandler.setHandler(staticResourceHandler);

		ServletContextHandler servletContextHandler = new ServletContextHandler(
				ServletContextHandler.NO_SESSIONS);
		servletContextHandler.setContextPath("/callback");
		servletContextHandler.addServlet(new ServletHolder(
				new DataServlet(this)), "/*");

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[] { staticContextHandler,
				servletContextHandler });

		// Add the handlers to the server and start jetty.
		server.setHandler(handlers);
		try {
			server.start();
			log.info("Started monitoring web server at port " + port);

		} catch (Exception e) {
			log.warn("Failed to start monitoring web server at port " + port, e);
		}
	}

	Buffer querylog = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(
			Constants.QUERYLOG_SIZE));

	@SuppressWarnings("unchecked")
	private void logQuery(Query q) {
		querylog.add(q);
	}

	@SuppressWarnings("unchecked")
	private List<Query> getLoggedQueries() {
		Query[] q = (Query[]) querylog.toArray(new Query[0]);
		return Arrays.asList(q);
	}

	private static class JdbcSocketHandler extends IoHandlerAdapter {

		private Coordinator coord;

		public JdbcSocketHandler(Coordinator c) {
			this.coord = c;
		}

		@Override
		public void messageReceived(IoSession session, Object message)
				throws Exception {
			String str = message.toString();
			if (!str.startsWith(Constants.MAGIC_HEADER)) {
				log.warn("No magic header in message, discarding.");
				session.write("!Use proper header");
				session.close(true);
				return;
			}
			str = str.substring(Constants.MAGIC_HEADER.length());
			if ("PING".equals(str.toUpperCase())) {
				session.write("PONG");
			}
			if (str.toUpperCase().startsWith("EXEC")) {
				String sql = str.substring(5);
				log.debug("QQ: " + sql);
				Query q = new Query(sql);

				List<NeverlandNode> nodes = coord.getCurrentNodes();
				if (nodes.size() < 1) {
					log.warn("No workers known, cannot continue");
					session.write("!No worker nodes known, sorry");
					session.close(false);
					return;
				}

				log.info("Received query #" + q.getId() + " " + q.getSql());
				q.setSubqueries(coord.getRewriter().rewrite(q, nodes.size()));
				Scheduler.SubquerySchedule schedule = coord.getScheduler()
						.schedule(q, nodes);
				List<ResultSet> resultSets = coord.getExecutor()
						.executeSchedule(schedule);
				ResultCombiner rc = coord.getCombiner();
				ResultSet aggrSet = rc.combine(schedule.getQuery(), resultSets);
				Coordinator.serializeResultSet(aggrSet, session);

				coord.logQuery(q);
			}
		}
	}

	protected void handle(String line) {
		log.info(line);
	}

	public ResultCombiner getCombiner() {
		return combiner;
	}

	// avoid race conditions by not allowing outsiders write access to the node
	// list
	public List<NeverlandNode> getCurrentNodes() {
		List<NeverlandNode> nnodes = new ArrayList<NeverlandNode>();
		try {
			for (String nodeid : zkg.getMemberIds()) {
				NeverlandNode nn = (NeverlandNode) SerializableUtils
						.fromByteArray(zkg.getMemberData(nodeid));
				nnodes.add(nn);
			}
		} catch (Exception e) {
			log.warn(e);
		}
		return Collections.unmodifiableList(nnodes);
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

	public Rewriter getRewriter() {
		return rewriter;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public Executor getExecutor() {
		return executor;
	}

	public static void serializeResultSet(ResultSet aggrSet, IoSession session) {
		// TODO: find a way to stream this
		String result = "";
		Map<Integer, Boolean> needsQuotes = new HashMap<Integer, Boolean>();
		try {
			ResultSetMetaData rsm = aggrSet.getMetaData();
			for (int i = 1; i <= rsm.getColumnCount(); i++) {
				result += rsm.getColumnName(i);
				if (i < rsm.getColumnCount()) {
					result += "\t";
				}
			}
			result += "\n";
			for (int i = 1; i <= rsm.getColumnCount(); i++) {
				String type = rsm.getColumnTypeName(i).toUpperCase();
				result += type;

				if (type.startsWith("VARCHAR") || type.equals("DATE")
						|| type.equals("TIME")) {
					needsQuotes.put(i, true);
				} else {
					needsQuotes.put(i, false);
				}
				if (i < rsm.getColumnCount()) {
					result += "\t";
				}
			}
			result += "\n";

			while (aggrSet.next()) {
				for (int i = 1; i <= rsm.getColumnCount(); i++) {
					if (needsQuotes.get(i)) {
						result += "\"";
					}
					result += aggrSet.getObject(i).toString();
					if (needsQuotes.get(i)) {
						result += "\"";
					}
					if (i < rsm.getColumnCount()) {
						result += "\t";
					}
				}
				result += "\n";
			}

			session.write(result);

		} catch (SQLException e) {
			log.warn(e);
		}
	}
}
