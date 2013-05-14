package nl.cwi.da.neverland.internal;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.apache.log4j.Logger;

public abstract class Rewriter {
	public abstract List<Subquery> rewrite(Query q, int numSubqueries)
			throws NeverlandException;

	private static Logger log = Logger.getLogger(Rewriter.class);

	public static class StupidRewriter extends Rewriter {
		@Override
		public List<Subquery> rewrite(Query q, int numSubqueries) {
			return Arrays.asList(new Subquery(q, q.getSql(), 0));
		}
	}

	public static class FactTable {
		@Override
		public String toString() {
			return "FactTable [schemaName=" + schemaName + ", name=" + name
					+ ", size=" + size + ", keyColumn=" + keyColumn
					+ ", keyColMin=" + keyColMin + ", keyColMax=" + keyColMax
					+ "]";
		}

		private String schemaName;
		private String name;
		private long size;
		private String keyColumn;
		private long keyColMin;
		private long keyColMax;
	}

	public static Rewriter constructRewriterFromDb(NeverlandNode nn)
			throws NeverlandException {

		Map<String, FactTable> factTables = new HashMap<String, FactTable>();

		try {
			log.info("Constructing rewriter from DB advertised by " + nn);
			Class.forName(Constants.JDBC_DRIVER);
			Connection c = DriverManager.getConnection(nn.getJdbcUrl(),
					nn.getJdbcUser(), nn.getJdbcPass());
			DatabaseMetaData dmd = c.getMetaData();
			Statement s = c.createStatement();
			ResultSet rs = dmd.getTables(null, null, "%", null);

			while (rs.next()) {
				String schemaName = rs.getString(2);
				String tableName = rs.getString(3);

				ResultSet countRes = s.executeQuery("SELECT COUNT(*) FROM "
						+ schemaName + "." + tableName + ";");
				if (!countRes.next()) {
					continue;
				}
				long tableSize = countRes.getLong(1);
				if (tableSize < 1) {
					continue;
				}

				ResultSet pkSet = dmd.getPrimaryKeys(null, schemaName,
						tableName);
				String keyCol = null;
				long minKey = 0;
				long maxKey = 0;
				long maxKeyDist = 0;

				while (pkSet.next()) {
					String tKeyCol = pkSet.getString(4);

					ResultSet keyRangeRes = s.executeQuery("SELECT MIN("
							+ tKeyCol + "),MAX(" + tKeyCol + ") FROM "
							+ schemaName + "." + tableName + ";");

					long keyDist = 0;
					long tMinKey = 0;
					long tMaxKey = 0;
					if (keyRangeRes.next()) {
						tMinKey = keyRangeRes.getLong(1);
						tMaxKey = keyRangeRes.getLong(2);
						keyDist = tMaxKey - tMinKey;
					}
					if (keyDist > maxKeyDist) {
						keyCol = tKeyCol;
						minKey = tMinKey;
						maxKey = tMaxKey;
						maxKeyDist = keyDist;
					}
				}
				if (keyCol == null) {
					continue;
				}

				if (!factTables.containsKey(schemaName)
						|| factTables.get(schemaName).size < tableSize) {
					FactTable ft = new FactTable();
					ft.name = tableName;
					ft.schemaName = schemaName;
					ft.size = tableSize;
					ft.keyColumn = keyCol;
					ft.keyColMin = minKey;
					ft.keyColMax = maxKey;

					factTables.put(schemaName, ft);
				}
			}
		} catch (Exception se) {
			throw new NeverlandException(se);
		}

		if (factTables.size() != 1) {
			throw new NeverlandException(
					"Sorry, but cannot find a fact table on " + nn);
		}

		FactTable ft = factTables.entrySet().iterator().next().getValue();
		log.info("Rewriting on " + ft);
		return new NotSoStupidRewriter(ft);
	}

	public static class NotSoStupidRewriter extends Rewriter {
		private String factTableName;
		private String factTableKey;
		private long factTableKeyMin;
		private long factTableKeyMax;

		// TODO: move numSubqueries to rewrite()?
		public NotSoStupidRewriter(FactTable ft) {
			this.factTableName = ft.name;
			this.factTableKey = ft.keyColumn;
			this.factTableKeyMin = ft.keyColMin;
			this.factTableKeyMax = ft.keyColMax;
		}

		public NotSoStupidRewriter(String factTable, String factTableKey,
				int factTableKeyMin, int factTableKeyMax) {
			this.factTableName = factTable.toLowerCase();
			this.factTableKey = factTableKey;
			this.factTableKeyMin = factTableKeyMin;
			this.factTableKeyMax = factTableKeyMax;
		}

		private static Logger log = Logger.getLogger(NotSoStupidRewriter.class);

		@Override
		public List<Subquery> rewrite(Query q, int numSubqueries)
				throws NeverlandException {

			if (numSubqueries < 2) {
				return new StupidRewriter().rewrite(q, 1);
			}

			if (!q.getTables().contains(factTableName)) {
				log.warn("Could not find fact table " + factTableName + " in "
						+ q.getSql());
				return Arrays.asList(new Subquery(q, q.getSql(), 0));
			}

			if (!q.isSingleSelect()) {
				log.warn("Did not find a single plain select in " + q.getSql());
				return Arrays.asList(new Subquery(q, q.getSql(), 0));
			}

			List<Subquery> subqueries = new ArrayList<Subquery>(numSubqueries);

			PlainSelect ps = q.getSingleSelect();
			if (q.needsCountsTable()) {
				SelectExpressionItem groupcount = new SelectExpressionItem();
				groupcount.setAlias(Constants.GROUP_NAME);
				Function countstar = new Function();
				countstar.setAllColumns(true);
				countstar.setName("COUNT");
				groupcount.setExpression(countstar);
				ps.getSelectItems().add(groupcount);
			}

			Expression oldWhere = ps.getWhere();

			for (int i = 0; i <= numSubqueries; i++) {

				long keyMin = factTableKeyMin + i
						* ((factTableKeyMax - factTableKeyMin) / numSubqueries);
				long keyMax = Math
						.min(factTableKeyMin
								+ (i + 1)
								* ((factTableKeyMax - factTableKeyMin) / numSubqueries),
								factTableKeyMax);

				if (keyMin == keyMax) {
					break;
				}

				// TODO: add some sort of verification that these do not overlap

				// min()/max()/sum() - easy
				// count() - easy, re-aggregate groups
				// avg() - need counts for each group
				// first() / last() - useful for boundaries - easy
				// distinct - easy

				/**
				 * Table 4-1: SQL99 Aggregate Functions AVG(expression)
				 * COUNT(expression) COUNT(*) MIN(expression) MAX(expression)
				 * SUM(expression)
				 **/

				Column c = new Column(new Table(factTableName, null),
						factTableKey);

				BinaryExpression bottomRange;
				if (i == 0) {
					bottomRange = new GreaterThanEquals();
				} else {
					bottomRange = new GreaterThan();
				}
				bottomRange.setLeftExpression(c);
				bottomRange.setRightExpression(new LongValue(Long
						.toString(keyMin)));

				BinaryExpression topRange = new MinorThanEquals();
				topRange.setLeftExpression(c);
				topRange.setRightExpression(new LongValue(Long.toString(keyMax)));

				AndExpression range = new AndExpression(bottomRange, topRange);
				if (oldWhere != null) {
					range = new AndExpression(new Parenthesis(oldWhere), range);
				}

				ps.setWhere(range);
				// ordering is not important here. We'll have to reorg the
				// result anyways.
				ps.setOrderByElements(null);

				// TODO: check if this is always allowed
				if (ps.getGroupByColumnReferences() != null) {
					ps.setLimit(null);
				}

				// TODO: do not add subqueries that are outside existing filter
				// conditions?
				subqueries.add(new Subquery(q, ps.toString(), i));
			}

			// right, let's rock!
			return subqueries;
		}

	}
}
