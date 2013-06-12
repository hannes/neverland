package nl.cwi.da.neverland.internal;


public class Constants {
	public static final int ZK_PORT = 50001;
	public static final String ZK_PREFIX = "/dbnodes";

	public static enum WorkerState {
		initializing, normal, shutdown
	};

	public static enum CoordinatorState {
		initializing, normal, shutdown
	};

	public static final long ADVERTISE_DELAY_MS = 1000;
	public static final long POLL_DELAY_MS = 1000;
	public static final int JDBC_PORT = 50002;

	public static final String MAGIC_HEADER = "dpfkg";
	public static final String GROUP_NAME = "__neverland_groupcount";
	public static final int MONITOR_PORT = 50003;
	public static final long DEFAULT_SHARD_SIZE = 1000000;
	public static final int QUERYLOG_SIZE = 10;

}
