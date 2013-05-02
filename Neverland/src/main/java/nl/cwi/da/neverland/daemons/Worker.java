package nl.cwi.da.neverland.daemons;

import java.io.IOException;

import nl.cwi.da.neverland.internal.Constants;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Worker extends Thread implements Watcher {

	protected ZooKeeper zkc;

	private static Logger log = Logger.getLogger(Worker.class);

	private Constants.WorkerState workerState = Constants.WorkerState.initializing;

	private String jdbcUri;
	private String zookeeper;

	public Worker(String zooKeeper, String jdbcUri) {
		this.zookeeper = zooKeeper;
		this.jdbcUri = jdbcUri;
	}

	@Override
	public void run() {
		try {
			zkc = new ZooKeeper(zookeeper, 2000, this);
		} catch (IOException e) {
			log.warn(e);
		}

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
						+ Constants.ADVERTISE_DELAY_MS + " ms", e);
			}

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
			} catch (InterruptedException e) {
				//
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO: do we need this at all?
	}

	public static void main(String[] args) {
		new Worker("localhost:" + Constants.ZK_PORT,
				"jdbc:log4jdbc:monetdb://localhost:50000/tpc-sf1").start();
	}
}
