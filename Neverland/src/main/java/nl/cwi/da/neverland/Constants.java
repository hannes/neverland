package nl.cwi.da.neverland;

public class Constants {
	public static final int ZK_PORT = 50001;
	public static final String ZK_PREFIX = "/dbnodes";
	public static final String JDBC_DRIVER = "nl.cwi.monetdb.jdbc.MonetDriver";

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

}
