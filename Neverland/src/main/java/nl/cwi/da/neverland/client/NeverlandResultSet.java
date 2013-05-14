package nl.cwi.da.neverland.client;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class NeverlandResultSet implements ResultSet {

	private Statement stmt = null;

	private String[] lines;
	private List<String> columns;
	private List<String> types;

	private static final int DATA_OFFSET = 2;

	private int rowPointer = DATA_OFFSET; // first two lines are headers

	public NeverlandResultSet(Statement s, String response) throws SQLException {
		stmt = s;
		lines = response.split("\n");
		if (lines.length < 2) {
			throw new SQLException("Meh");
		}
		columns = Arrays.asList(lines[0].toLowerCase().split("\t"));
		types = Arrays.asList(lines[1].toLowerCase().split("\t"));

		beforeFirst();
	}

	public int size() {
		return lines.length - DATA_OFFSET;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		if (row >= 0) {
			rowPointer = DATA_OFFSET + row;
		} else {
			rowPointer = DATA_OFFSET + size() + row;
		}
		return (rowPointer > DATA_OFFSET && rowPointer < lines.length);
	}

	@Override
	public void afterLast() throws SQLException {
		rowPointer = size() + 1;
	}

	@Override
	public void beforeFirst() throws SQLException {
		rowPointer = DATA_OFFSET - 1;
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException {
		// nop
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		columnLabel = columnLabel.toLowerCase();

		for (int i = 0; i < columns.size(); i++) {
			if (columns.get(i).equals(columnLabel)) {
				return i + 1;
			}
		}
		throw new SQLException("Column not found in result set - "
				+ columnLabel);
	}

	@Override
	public boolean first() throws SQLException {
		rowPointer = DATA_OFFSET;
		return rowPointer < lines.length;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no arrays");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no arrays");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	private String[] rowdata = null;
	private int rowdataIndex = -1;

	private String getData(int index) throws SQLException {
		if (rowPointer < DATA_OFFSET || rowPointer >= lines.length) {
			throw new SQLException("Row index out of bounds");
		}

		if (rowdataIndex != rowPointer) {
			rowdata = lines[rowPointer].split("\t");
		}
		index = index - 1;
		if (index < 0 || index >= rowdata.length) {
			throw new SQLException("Column index out of bounds");
		}
		return rowdata[index];
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return new BigDecimal(getData(columnIndex));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		// hm? scale?
		return getBigDecimal(columnIndex);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		// hm? scale?
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no blobs");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no blobs");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return Boolean.parseBoolean(getData(columnIndex));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return Byte.valueOf(getData(columnIndex));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(findColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no byte arrays");

	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no byte arrays");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no clobs");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no clobs");
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLException("Unsupported, no cursors");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return Date.valueOf(getData(columnIndex));
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		// TODO ??
		return null;
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		// TODO ??
		return null;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return Double.valueOf(getData(columnIndex));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return Integer.MAX_VALUE;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return Float.valueOf(getData(columnIndex));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return Integer.parseInt(getData(columnIndex));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return Long.parseLong(getData(columnIndex));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new NeverlandResultSetMetaData(columns, types);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no N* types");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no N* types");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no N* types");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no N* types");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no N* types");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no N* types");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getData(columnIndex);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLException("Unsupported, no objects");
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("Unsupported, no objects");
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject(findColumn(columnLabel));
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		throw new SQLException("Unsupported, no objects");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("Unsupported, no objects");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no refs");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no refs");
	}

	@Override
	public int getRow() throws SQLException {
		return rowPointer - DATA_OFFSET + 1;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no row ids");

	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no row ids");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return Short.valueOf(getData(columnIndex));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no objects");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no objects");

	}

	@Override
	public Statement getStatement() throws SQLException {
		return stmt;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		String str = getData(columnIndex);
		// TODO: un-escape
		return str.substring(1, str.length() - 1);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return Time.valueOf(getString(columnIndex));
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException("cal parameter is not supported");
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException("cal parameter is not supported");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return Timestamp.valueOf(getString(columnIndex));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new SQLException("cal parameter is not supported");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new SQLException("cal parameter is not supported");
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, no streams");
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		try {
			return new URL(getString(columnIndex));
		} catch (MalformedURLException e) {
			throw new SQLException("Unable to parse URL");
		}
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return getURL(findColumn(columnLabel));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return rowPointer < DATA_OFFSET;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return rowPointer == DATA_OFFSET;

	}

	@Override
	public boolean isLast() throws SQLException {
		return rowPointer == lines.length - 1;

	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("WTF");
	}

	@Override
	public boolean last() throws SQLException {
		rowPointer = lines.length - 1;
		return lines.length > 3;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public boolean next() throws SQLException {
		rowPointer++;
		return rowPointer < lines.length;
	}

	@Override
	public boolean previous() throws SQLException {
		rowPointer--;
		return rowPointer >= DATA_OFFSET;
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		rowPointer += rows;
		if (rowPointer < DATA_OFFSET)
			rowPointer = DATA_OFFSET - 1;
		if (rowPointer > lines.length)
			rowPointer = lines.length;
		return rowPointer >= DATA_OFFSET && rowPointer < lines.length;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new SQLException("Only forward fetch is supported");
		}

	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLException(
				"Setting fetch size is not supported. Change your query!");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("WTF");
	}

	// below are update methods, which are all unsupported.

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");

	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new SQLException("Unsupported, read-only connection");
	}

	@Override
	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
