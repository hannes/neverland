package nl.cwi.da.neverland.tests;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;

import com.sun.rowset.CachedRowSetImpl;

public class TestResultSet extends CachedRowSetImpl {
	private static final long serialVersionUID = 1L;

	public TestResultSet() throws SQLException {
		super();
	}
	
	public TestResultSet(TestResultSet dad) throws SQLException {
		populate(dad);
		dad.beforeFirst();
		beforeFirst();
	}

	public TestResultSet(List<String> columnNames,
			List<Integer> columnTypesSql, List<String> columnTypeNames, int size)
			throws SQLException {
		super();

		RowSetMetaData rwsm = new RowSetMetaDataImpl();
		rwsm.setColumnCount(columnNames.size());
		for (int i = 1; i <= columnNames.size(); i++) {
			rwsm.setColumnName(i, columnNames.get(i - 1));
			rwsm.setColumnType(i, columnTypesSql.get(i - 1));
			rwsm.setColumnTypeName(i, columnTypeNames.get(i - 1));
		}
		this.setMetaData(rwsm);

		moveToInsertRow();
		for (int r = 0; r < size; r++) {
			for (int c = 1; c <= columnNames.size(); c++) {
				updateObject(c, Types.NULL);
			}
			insertRow();
		}
		moveToCurrentRow();
		beforeFirst();
	}

	public void setColumn(int c, List<Object> values) throws SQLException {
		if (values.size() > size()) {
			throw new RuntimeException("Out of capacity");
		}
		first();
		for (int r = 0; r < values.size(); r++) {
			updateObject(c, values.get(r));
			updateRow();
			next();
		}
		beforeFirst();
	}
}
