package nl.cwi.da.neverland.internal;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class InternalResultSet extends AbstractResultSet {

	private List<Object[]> rows = new ArrayList<Object[]>();
	private Object[] row = null;

	private boolean onInsertRow = false;

	private static class InternalResultSetColumn {
		private String columnName;
		private String dbType;
		private int jdbcType;

		public InternalResultSetColumn(String columnName, String dbType,
				int jdbcType) {
			this.columnName = columnName;
			this.dbType = dbType;
			this.jdbcType = jdbcType;
		}

		public String toString() {
			return "RSC: " + columnName + " (" + dbType + ")";
		}

		public InternalResultSetColumn() {
		}
	}

	private List<InternalResultSetColumn> columns = new ArrayList<InternalResultSetColumn>();

	private int curRow = -1;

	public InternalResultSet() {
		// move along...
	}

	public ResultSet addRow(Object[] row) {
		rows.add(row);
		return this;
	}

	public InternalResultSet(ResultSet source) throws SQLException {
		if (source instanceof InternalResultSet) {
			rows = new ArrayList<Object[]>(((InternalResultSet) source).size());
		}
		setMetaData(source.getMetaData());
		add(source);
		if (source instanceof InternalResultSet) {
			source.beforeFirst();
		}
	}

	protected void setMetaData(ResultSetMetaData metaData) throws SQLException {
		columns.clear();
		for (int c = 1; c <= metaData.getColumnCount(); c++) {
			InternalResultSetColumn cc = new InternalResultSetColumn(
					metaData.getColumnName(c), metaData.getColumnTypeName(c),
					metaData.getColumnType(c));
			columns.add(cc);
		}
	}

	public int getColumnCount() {
		return columns.size();
	}

	public int getConcurrencyType() {
		return ResultSet.CONCUR_READ_ONLY;
	}

	public int getType() {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	public String getColumnName(int jdbcColIndex) throws SQLException {
		return getCol(jdbcColIndex).columnName;
	}

	public int getColumnIndex(String columnName) throws SQLException {
		for (int c = 1; c <= getColumnCount(); c++) {
			InternalResultSetColumn col = getCol(c);
			if (col.columnName.toLowerCase().equals(columnName.toLowerCase())) {
				return c;
			}
		}
		throw new SQLException("Column not found");
	}

	public String getColumnTypeName(int jdbcColIndex) throws SQLException {
		return getCol(jdbcColIndex).dbType;
	}

	public int getColumnType(int jdbcColIndex) throws SQLException {
		return getCol(jdbcColIndex).jdbcType;
	}

	private InternalResultSetColumn getCol(int jdbcColIndex)
			throws SQLException {
		if (jdbcColIndex > columns.size()) {
			throw new SQLException("Column index out of bounds");
		}
		return columns.get(jdbcColIndex - 1);
	}

	public ResultSetMetaData getMetaData() {
		return this;
	}

	public InternalResultSet add(ResultSet rs) throws SQLException {
		moveToInsertRow();
		while (rs.next()) {
			for (int i = 1; i <= getColumnCount(); i++) {
				updateObject(i, rs.getObject(i));
			}
			insertRow();
		}
		beforeFirst();
		return this;
	}

	public int size() {
		return rows.size();
	}

	public boolean next() {
		curRow++;
		row = null;
		return isRowValid();
	}

	public void moveToInsertRow() {
		onInsertRow = true;
	}

	public void moveToCurrentRow() {
		onInsertRow = false;
		insertRow = null;
	}

	public void beforeFirst() {
		curRow = -1;
	}

	Object[] insertRow = null;

	public void updateObject(int jdbcColIndex, Object value)
			throws SQLException {
		if (!onInsertRow) {
			throw new SQLException("Updates only in insert mode");
		}
		if (jdbcColIndex > columns.size()) {
			throw new SQLException("Col index out of bounds");
		}
		if (insertRow == null) {
			insertRow = new Object[columns.size()];
		}
		insertRow[jdbcColIndex - 1] = value;
	}

	// only really used in test cases
	public InternalResultSet setObject(int jdbcColIndex, int jdbcRowIndex,
			Object value) throws SQLException {
		if (jdbcColIndex > columns.size()) {
			throw new SQLException("Col index out of bounds");
		}
		if (jdbcRowIndex > size()) {
			throw new SQLException("Row index out of bounds (size: " + size()
					+ ", index:" + jdbcRowIndex);
		}
		rows.get(jdbcRowIndex - 1)[jdbcColIndex - 1] = value;
		return this;
	}

	public void insertRow() throws SQLException {
		if (!onInsertRow) {
			throw new SQLException("Updates only in insert mode");
		}
		rows.add(insertRow);
		insertRow = null;
	}

	private boolean isRowValid() {
		return curRow >= 0 && curRow < rows.size();
	}

	public int getInt(int jdbcColIndex) throws SQLException {
		return (Integer) getObject(jdbcColIndex);
	}

	public String getString(int jdbcColIndex) throws SQLException {
		return (String) getObject(jdbcColIndex);
	}

	public long getLong(int jdbcColIndex) throws SQLException {
		return (Long) getObject(jdbcColIndex);
	}

	public float getFloat(int jdbcColIndex) throws SQLException {
		return ((Double) getObject(jdbcColIndex)).floatValue();
	}

	public double getDouble(int jdbcColIndex) throws SQLException {
		return (Double) getObject(jdbcColIndex);
	}

	public Object getObject(int colIndex) throws SQLException {
		if (!isRowValid()) {
			throw new SQLException("Row index out of bounds");
		}
		if (row == null) {
			row = rows.get(curRow);
		}
		if (colIndex > row.length) {
			throw new SQLException("Col index out of bounds");
		}
		return row[colIndex - 1];
	}

	public String toString() {
		String ret = "RS: ";
		for (Object[] row : rows) {
			ret += Arrays.toString(row) + "\n";
		}
		return ret;
	}

	public InternalResultSet setColumnCount(int columnCount) {
		columns = new ArrayList<InternalResultSetColumn>(columnCount);
		for (int c = 0; c < columnCount; c++) {
			columns.add(new InternalResultSetColumn());
		}
		return this;
	}

	public InternalResultSet setColumnName(int i, String columnName)
			throws SQLException {
		getCol(i).columnName = columnName;
		return this;
	}

	public InternalResultSet setColumnType(int i, int columnType)
			throws SQLException {
		getCol(i).jdbcType = columnType;
		return this;
	}

	public InternalResultSet setColumnTypeName(int i, String columnTypeName)
			throws SQLException {
		getCol(i).dbType = columnTypeName;
		return this;
	}

	public InternalResultSet limit(Query q) throws SQLException {
		Limit l = q.getSingleSelect().getLimit();

		if (l.getOffset() + l.getRowCount() > rows.size()) {
			throw new SQLException("Trying to get " + l
					+ " from result set with " + rows.size() + " rows.");
		}
		InternalResultSet result = new InternalResultSet();
		result.setMetaData(this.getMetaData());
		for (int row = (int) l.getOffset(); row < Math.min(
				l.getOffset() + l.getRowCount(), rows.size()); row++) {
			result.addRow(rows.get(row));
		}
		return result;
	}

	public InternalResultSet sort(Query q) throws SQLException {
		if (!q.isSingleSelect()) {
			throw new SQLException(
					"Unable to sort anything else than a plain select");
		}

		@SuppressWarnings("unchecked")
		// JSQL parser does not have generics
		List<Object> obes = q.getSingleSelect().getOrderByElements();
		if (obes == null) {
			return this;
		}

		// list of result set colums to order by.
		final List<Integer> sortCols = new ArrayList<Integer>();
		final Map<Integer, Boolean> sortColsAsc = new HashMap<Integer, Boolean>();

		for (Object e : obes) {
			OrderByElement obe = (OrderByElement) e;
			Expression obee = obe.getExpression();
			if (obee instanceof Column) {
				Column oc = (Column) obee;
				int cindex = getColumnIndex(oc.getColumnName());
				sortColsAsc.put(cindex - 1, obe.isAsc());
				sortCols.add(cindex - 1);
			} else {
				throw new SQLException("Cannot order by non-columns!");
			}
		}

		Comparator<Object> rowSorter = new Comparator<Object>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public int compare(Object o1, Object o2) {
				Object[] row1 = (Object[]) o1;
				Object[] row2 = (Object[]) o2;

				for (int index : sortCols) {
					Object e1 = row1[index];
					Object e2 = row2[index];
					if (!(e1 instanceof Comparable)
							|| !(e2 instanceof Comparable)) {
						return 0;
					}
					Comparable c1 = (Comparable) e1;
					Comparable c2 = (Comparable) e2;

					if (c1.equals(c2)) {
						continue;
					}
					int cmp = c1.compareTo(c2);
					if (sortColsAsc.get(index)) {
						return cmp;
					} else {
						return cmp *= -1;
					}
				}
				return 0;
			}
		};
		Collections.sort(rows, rowSorter);
		return this;
	}
}
