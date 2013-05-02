package nl.cwi.da.neverland.client;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class NeverlandResultSetMetaData implements ResultSetMetaData {

	private List<String> columns;
	private List<String> types;

	public NeverlandResultSetMetaData(List<String> columns, List<String> types) {
		this.columns = columns;
		this.types = types;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return columns.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false; // might be wrong, but read only, who cares
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true; // we do not have case-insensitive data
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return true; // in theory, every column can be used in a where-clause
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return false; // we do not know....
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return true; // does it matter?
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return 255;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		column = column-1;
		if (column < 0 || column >= columns.size()) {
			throw new SQLException("Column index out of bounds");
		}
		return columns.get(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return getColumnLabel(column);
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return ""; // not so important, user knows this anyway
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return 0; // we could know, but do not care so much.
	}

	@Override
	public int getScale(int column) throws SQLException {
		return 0; // we could know, but do not care so much.
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return ""; // not so important, user knows this anyway
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return ""; // not so important, user knows this anyway
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		String sqltype = getColumnTypeName(column);
		if (sqltype.contains("(")) {
			sqltype = sqltype.substring(0, sqltype.indexOf("("));
		}
		sqltype=sqltype.toUpperCase();
		Integer jdbctype = null;
		try {
			jdbctype = Types.class.getField(sqltype).getInt(null);
		} catch (Exception e) {
			throw new SQLException("Unable to find JDB type for column type "
					+ sqltype, e);
		}
		return jdbctype;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return types.get(column-1);
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		// TODO make me sometime
		return null;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("WTF");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("WTF");
	}

}