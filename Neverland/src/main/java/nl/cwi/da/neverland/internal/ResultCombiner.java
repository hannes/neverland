package nl.cwi.da.neverland.internal;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;

import org.apache.log4j.Logger;

import com.sun.rowset.CachedRowSetImpl;

public abstract class ResultCombiner {
	public abstract ResultSet combine(Query q, List<ResultSet> sets)
			throws NeverlandException;

	public static class SmartResultCombiner extends ResultCombiner {
		private ResultCombiner concat = new ConcatResultCombiner();
		private ResultCombiner merge = new AggregationResultCombiner();

		@Override
		public ResultSet combine(Query q, List<ResultSet> sets)
				throws NeverlandException {

			if (q.isSingleSelect() && q.hasGrouping()) {
				return merge.combine(q, sets);
			} else
				return concat.combine(q, sets);
		}

	}

	public static class AggregationResultCombiner extends ResultCombiner {

		private static Logger log = Logger
				.getLogger(AggregationResultCombiner.class);

		private static class AggregationGroup extends HashMap<Integer, Object> {
			private static final long serialVersionUID = 1L;
		}

		private static class AggregationValue extends HashMap<Integer, Object> {
			private static final long serialVersionUID = 1L;

			public void merge(Query q, AggregationValue av) {
				for (Entry<Integer, Object> ov : entrySet()) {

					Number nvv = (Number) av.get(ov.getKey());
					Number ovv = (Number) ov.getValue();
					Number nv = 0;

					switch (q.getAggrType(ov.getKey())) {
					case AVG:
						log.warn("AVG not supported yet");
						break;
					case COUNT:
						nv = nvv.doubleValue() + ovv.doubleValue();
						break;
					case MAX:
						nv = Math.min(nvv.doubleValue(), nvv.doubleValue());
						break;
					case MIN:
						nv = Math.min(nvv.doubleValue(), nvv.doubleValue());
						break;
					case SUM:
						nv = nvv.doubleValue() + ovv.doubleValue();
						break;
					default:
						log.warn("errrm");

						break;
					}

					Number nvo = null;

					if (ovv instanceof Integer) {
						nvo = nv.intValue();
					}
					if (ovv instanceof Long) {
						nvo = nv.longValue();
					}
					if (ovv instanceof Float) {
						nvo = nv.floatValue();
					}
					if (ovv instanceof Double) {
						nvo = nv.doubleValue();
					}
					if (nvo == null) {
						log.warn("Unsupported column type" + ovv.getClass());
					}

					put(ov.getKey(), nvo);

				}
			}

		}

		@Override
		public ResultSet combine(Query q, List<ResultSet> sets)
				throws NeverlandException {

			if (sets.size() < 1) {
				throw new NeverlandException("Need at least one result set");
			}

			CachedRowSet crs = null;

			// hash map for aggregations
			Map<AggregationGroup, AggregationValue> aggregationMap = new HashMap<AggregationGroup, AggregationValue>();

			try {
				crs = new CachedRowSetImpl();
				ResultSetMetaData rsm = sets.get(0).getMetaData();

				// idiotic conversion to give meta data to the cached result set
				RowSetMetaData rwsm = new RowSetMetaDataImpl();
				rwsm.setColumnCount(rsm.getColumnCount());
				for (int i = 1; i <= rsm.getColumnCount(); i++) {
					rwsm.setColumnName(i, rsm.getColumnName(i));
					rwsm.setColumnType(i, rsm.getColumnType(i));
					rwsm.setColumnTypeName(i, rsm.getColumnTypeName(i));
				}
				crs.setMetaData(rwsm);

				// now walk through all result sets
				for (ResultSet rs : sets) {
					while (rs.next()) {
						AggregationGroup ag = new AggregationGroup();
						AggregationValue av = new AggregationValue();
						for (int c = 1; c <= rsm.getColumnCount(); c++) {
							if (q.isGroupKey(c)) {
								ag.put(c, rs.getObject(c));
							} else {
								av.put(c, rs.getObject(c));
							}
						}
						if (!aggregationMap.containsKey(ag)) {
							aggregationMap.put(ag, av);
						} else {
							aggregationMap.get(ag).merge(q, av);
						}
					}
				}

				crs.moveToInsertRow();

				for (Entry<AggregationGroup, AggregationValue> e : aggregationMap
						.entrySet()) {
					for (Entry<Integer, Object> eg : e.getKey().entrySet()) {
						crs.updateObject(eg.getKey(), eg.getValue());
					}
					for (Entry<Integer, Object> eg : e.getValue().entrySet()) {
						crs.updateObject(eg.getKey(), eg.getValue());
					}
					crs.insertRow();
				}
				crs.moveToCurrentRow();
				crs.beforeFirst();

			} catch (SQLException e) {
				throw new NeverlandException("Failed to combine results for "
						+ q, e);
			}

			return crs;
		}
	}

	public static class ConcatResultCombiner extends ResultCombiner {

		@Override
		public ResultSet combine(Query q, List<ResultSet> sets)
				throws NeverlandException {
			CachedRowSet crs = null;

			if (sets.size() < 1) {
				throw new NeverlandException("Need at least one result set");
			}

			try {
				crs = new CachedRowSetImpl();

				crs.populate(sets.get(0));
				crs.moveToInsertRow();

				ResultSetMetaData rsm = crs.getMetaData();

				for (int r = 1; r < sets.size(); r++) {
					ResultSet rs = sets.get(r);
					rs.beforeFirst();
					while (rs.next()) {
						for (int i = 1; i <= rsm.getColumnCount(); i++) {
							crs.updateObject(i, rs.getObject(i));
						}
						crs.insertRow();
					}
				}
				crs.moveToCurrentRow();
				crs.beforeFirst();
			} catch (SQLException e) {
				throw new NeverlandException("Unable to combine result sets", e);
			}
			return crs;
		}
	}

	public static String getSchema(String tableName, ResultSet rs)
			throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		StringBuilder sb = new StringBuilder(1024);
		if (columnCount > 0) {
			sb.append("CREATE TABLE ").append(tableName).append(" ( ");
		}
		for (int i = 1; i <= columnCount; i++) {
			if (i > 1)
				sb.append(", ");
			String columnName = rsmd.getColumnLabel(i);
			String columnType = rsmd.getColumnTypeName(i);

			sb.append(columnName).append(" ").append(columnType);

			int precision = rsmd.getPrecision(i);
			if (precision != 0) {
				sb.append("( ").append(precision).append(" )");
			}
		}
		sb.append(" ) ");
		return sb.toString();
	}

	public static void printResultSet(ResultSet rs, PrintStream ps) {
		try {
			ResultSetMetaData rsm = rs.getMetaData();
			for (int i = 1; i <= rsm.getColumnCount(); i++) {
				ps.print(rsm.getColumnName(i));
				if (i < rsm.getColumnCount()) {
					ps.print("\t");
				}
			}
			ps.print("\n");
			for (int i = 1; i <= rsm.getColumnCount(); i++) {
				ps.print(rsm.getColumnTypeName(i).toUpperCase());
				if (i < rsm.getColumnCount()) {
					ps.print("\t");
				}
			}
			ps.print("\n");

			while (rs.next()) {
				for (int i = 1; i <= rsm.getColumnCount(); i++) {
					ps.print(rs.getObject(i).toString());
					if (i < rsm.getColumnCount()) {
						ps.print("\t");
					}
				}
				ps.print("\n");
			}
			rs.beforeFirst();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
