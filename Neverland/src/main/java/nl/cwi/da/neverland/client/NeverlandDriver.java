package nl.cwi.da.neverland.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class NeverlandDriver implements Driver {

	// register driver with JDBC
	static {
		try {
			DriverManager.registerDriver(new NeverlandDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static Logger log = Logger.getLogger(NeverlandDriver.class);

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			throw new SQLException("Unsupported URL " + url);
		}
		log.info("Connecting to " + url);
		try {
			URI u = new URI(url.substring(5));
			@SuppressWarnings("resource")
			Socket socket = new Socket(u.getHost(), u.getPort());
			Writer out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			return new NeverlandConnection(in, out);
		} catch (Exception e) {
			throw new SQLException("Unable to connect to " + url, e);
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.toLowerCase().startsWith("jdbc:neverland://");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 42;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public java.util.logging.Logger getParentLogger()
			throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

}
