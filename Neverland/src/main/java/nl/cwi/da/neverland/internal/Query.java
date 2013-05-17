package nl.cwi.da.neverland.internal;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.apache.log4j.Logger;

public class Query {
	private static long qid = 1;
	private long queryId;
	private double timeTaken;
	private List<Subquery> subqueries;

	private static Logger log = Logger.getLogger(Query.class);

	private static CCJSqlParserManager parser = new CCJSqlParserManager();

	protected String sqlQuery;

	private Map<Integer, Boolean> groupCols = new HashMap<Integer, Boolean>();
	private Map<Integer, AggregationType> aggrCols = new HashMap<Integer, AggregationType>();
	private List<String> tables = new ArrayList<String>();
	
	// transient to keep them from being serialized
	private transient Statement parsedQuery = null;
	private transient List<PlainSelect> selects = new ArrayList<PlainSelect>();
	private transient PlainSelect singleSelect = null;

	private boolean isSingleSelect;
	private boolean needsCounts = false;

	public enum AggregationType {
		MIN, MAX, AVG, SUM, COUNT, FIRST, LAST
	};

	public Query() {
		this.queryId = createId();
	}

	private static synchronized long createId() {
		return qid++;
	}

	public Query(String sqlQuery) throws NeverlandException {
		this.queryId = createId();

		this.sqlQuery = sqlQuery;

		try {
			parsedQuery = parser.parse(new StringReader(sqlQuery));
		} catch (JSQLParserException e) {
			String reason = "Failed to parse query " + sqlQuery;
			throw new NeverlandException(reason);
		}

		parsedQuery.accept(new SelectStatementVisitor() {
			@Override
			public void visit(Select sq) {
				sq.getSelectBody().accept(new SelectVisitor() {
					@Override
					public void visit(PlainSelect plainSelect) {
						selects.add(plainSelect);

						if (plainSelect.getJoins() != null) {
							for (Object o : plainSelect.getJoins()) {
								FromItem fo = ((Join) o).getRightItem();
								if (fo instanceof Table) {
									tables.add(((Table) fo).getName()
											.toLowerCase());
								}

							}
						}

						plainSelect.getFromItem().accept(new FromItemVisitor() {

							@Override
							public void visit(SubJoin subjoin) {
								log.warn("Subjoins not supported, sorry.");
							}

							@Override
							public void visit(SubSelect subSelect) {
								log.warn("Subselects not supported, sorry.");
							}

							@Override
							public void visit(Table tableName) {
								tables.add(tableName.getName().toLowerCase());
							}
						});

					}

					@Override
					public void visit(Union union) {
						log.warn("Union not supported, sorry.");
					}

				});
			}
		});

		isSingleSelect = selects.size() == 1;
		if (isSingleSelect()) {
			singleSelect = selects.get(0);
		}

		// houston we have a grouping with an avg aggregation function
		// add the __groupcount variable to properly calculate avg
		// also, find out which results columns are grouped
		int col = 1; // JDBC indices...
		for (Object o : singleSelect.getSelectItems()) {
			SelectExpressionItem si = (SelectExpressionItem) o;
			Expression sie = si.getExpression();
			if (sie instanceof Function) {
				String fun = ((Function) sie).getName().toUpperCase();
				if (fun.equals("AVG") || fun.equals("COUNT")) {
					needsCounts = true;
				}
				Query.AggregationType aggrType = Enum.valueOf(
						Query.AggregationType.class, fun);
				setAggregated(col, aggrType);
			}
			if (sie instanceof Column
					&& singleSelect.getGroupByColumnReferences() != null) {
				setGroup(col);
			}
			col++;
		}

	}

	public String getSql() {
		return sqlQuery;
	}

	@Override
	public String toString() {
		return sqlQuery;
	}

	@Override
	public boolean equals(Object o) {
		return this.toString().equals(o.toString());
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	public void setAggregated(int col, AggregationType fun) {
		aggrCols.put(col, fun);
	}

	public void setGroup(int col) {
		groupCols.put(col, true);
	}

	public List<String> getTables() {
		return tables;
	}

	public Statement getStatement() {
		return parsedQuery;
	}

	public List<PlainSelect> getPlainSelects() {
		return selects;
	}

	public boolean isSingleSelect() {
		return isSingleSelect;
	}

	public boolean needsCountsTable() {
		return needsCounts;
	}

	public PlainSelect getSingleSelect() {
		return singleSelect;
	}

	public boolean isGroupKey(int jdbcIndex) {
		return groupCols.containsKey(jdbcIndex);
	}

	public AggregationType getAggrType(int jdbcIndex) {
		return aggrCols.get(jdbcIndex);
	}

	public boolean isAggregatedResultColumn(int jdbcIndex) {
		return aggrCols.containsKey(jdbcIndex);
	}

	public Map<Integer, AggregationType> getAggregationCols() {
		return Collections.unmodifiableMap(aggrCols);
	}

	public Map<Integer, Boolean> getGroupCols() {
		return Collections.unmodifiableMap(groupCols);
	}

	public boolean hasGrouping() {
		return singleSelect.getGroupByColumnReferences() != null;
	}

	public boolean needsAggregation() {
		return aggrCols.size() > 0;
	}

	public long getId() {
		return queryId;
	}

	public double getTimeTaken() {
		return timeTaken;
	}

	public void setTimeTaken(double timeTaken) {
		this.timeTaken = timeTaken;
	}

	public List<Subquery> getSubqueries() {
		return subqueries;
	}

	public void setSubqueries(List<Subquery> subqueries) {
		this.subqueries = subqueries;
	}
}
