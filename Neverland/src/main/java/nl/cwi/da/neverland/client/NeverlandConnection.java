package nl.cwi.da.neverland.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;

import nl.cwi.da.neverland.Constants;

public class NeverlandConnection implements Connection {

	private static Logger log = Logger.getLogger(NeverlandConnection.class);

	private BufferedReader in;
	private Writer out;
	boolean connected = true;

	public NeverlandConnection(BufferedReader in, Writer out)
			throws SQLException {
		this.in = in;
		this.out = out;

		if (!isValid(0)) {
			throw new SQLException(
					"Protocol error. Are you sure you are connecting to a Neverland server on the JDBC port?");
		}
		connected = true;
	}

	public String request(String req) {
		String response = "";
		try {
			out.write(Constants.MAGIC_HEADER + req + "\n");
			out.flush();
			do {
				response += in.readLine() + "\n";
			} while (in.ready());
		} catch (Exception e) {
			log.warn("Communication error", e);
		}
		log.info(response);
		
		return response;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return request("PING").equals("PONG\n");
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new NeverlandStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new NeverlandStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLException("Unsupported, no stored procecures");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return sql + " -- no idea what is native on the wrapped DBs!"; // haha
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return false;
	}

	@Override
	public void commit() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		// TODO do something here!
		return null;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		if (!readOnly) {
			throw new SQLException("Unsupported, read-only connection");
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return true;
	}

	@Override
	public void close() throws SQLException {
		connected = false;
		try {
			in.close();
			out.close();
		} catch (IOException e) {
			log.debug(e);
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return !connected;
	}

	private String catalog;

	@Override
	public void setCatalog(String catalog) throws SQLException {
		this.catalog = catalog;

	}

	@Override
	public String getCatalog() throws SQLException {
		return catalog;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO: should we do warnings?
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return new NeverlandStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return new NeverlandStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new SQLException("Unsupported, no stored procecures");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLException("Unsupported, only default types");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLException("Unsupported, only default types");
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
	}

	@Override
	public int getHoldability() throws SQLException {
		return Integer.MAX_VALUE;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new NeverlandStatement(this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new NeverlandStatement(this, sql);

	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLException("Unsupported, no stored procecures");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return new NeverlandStatement(this, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return new NeverlandStatement(this, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return new NeverlandStatement(this, sql);
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLException("Unsupported, no CLOBs");
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLException("Unsupported, no BLOBs");

	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLException("Unsupported, no NCLOBs");

	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLException("Unsupported, no SQLXMLs");
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		clientInfoProperties.setProperty(name, value);
	}

	private Properties clientInfoProperties = new Properties();

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		clientInfoProperties.putAll(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return (String) clientInfoProperties.get(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return clientInfoProperties;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		throw new SQLException("Unsupported, no JDBC Arrays");

	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		throw new SQLException("Unsupported, no JDBC Structs");

	}

	@Override
	public void abort(Executor executor) throws SQLException {
		this.close();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		throw new SQLException("Unsupported, no timeouts");
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new SQLException("Unsupported, no timeouts");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("WTF");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("WTF");
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLException("Unsupported, set schema in JDBC URI");
	}

	@Override
	public String getSchema() throws SQLException {
		throw new SQLException("Unsupported, set schema in JDBC URI");
	}

}
