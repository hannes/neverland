package nl.cwi.da.neverland.internal;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.apache.log4j.Logger;

public abstract class Rewriter {
	public abstract List<Subquery> rewrite(Query q) throws NeverlandException;

	public static class StupidRewriter extends Rewriter {
		@Override
		public List<Subquery> rewrite(Query q) {
			return Arrays.asList(new Subquery(q, q.getSql(), 0));
		}
	}

	public static class NotSoStupidRewriter extends Rewriter {
		private String factTableName;
		private String factTableKey;
		private int factTableKeyMin;
		private int factTableKeyMax;
		private int numSubqueries;
		private CCJSqlParserManager parser;

		public NotSoStupidRewriter(String factTable, String factTableKey,
				int factTableKeyMin, int factTableKeyMax, int numSubqueries) {
			this.factTableName = factTable.toLowerCase();
			this.factTableKey = factTableKey;
			this.factTableKeyMin = factTableKeyMin;
			this.factTableKeyMax = factTableKeyMax;
			this.numSubqueries = numSubqueries;
			this.parser = new CCJSqlParserManager();
		}

		private static Logger log = Logger.getLogger(NotSoStupidRewriter.class);

		@Override
		public List<Subquery> rewrite(Query q) throws NeverlandException {
			Statement pq = null;
			try {
				pq = parser.parse(new StringReader(q.getSql()));
			} catch (JSQLParserException e) {
				String reason = "Failed to parse query " + q.getSql();
				throw new NeverlandException(reason);
			}

			final Collection<String> tables = new ArrayList<String>();
			final Collection<PlainSelect> selects = new ArrayList<PlainSelect>();

			pq.accept(new SelectStatementVisitor() {
				@Override
				public void visit(Select sq) {
					sq.getSelectBody().accept(new SelectVisitor() {
						@Override
						public void visit(PlainSelect plainSelect) {
							selects.add(plainSelect);

							for (Object o : plainSelect.getJoins()) {
								FromItem fo = ((Join) o).getRightItem();
								if (fo instanceof Table) {
									tables.add(((Table) fo).getName()
											.toLowerCase());
								}

							}

							plainSelect.getFromItem().accept(
									new FromItemVisitor() {

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
											tables.add(tableName.getName()
													.toLowerCase());
										}
									});

							// TODO: find existing restrictions on fact table?
							// plainSelect.getWhere().accept(new
							// ExpressionVisitor ...)
						}

						@Override
						public void visit(Union union) {
							log.warn("Union not supported, sorry.");
						}

					});
				}
			});

			if (!tables.contains(factTableName)) {
				log.warn("Could not find fact table " + factTableName + " in "
						+ q.getSql());
				return Arrays.asList(new Subquery(q, q.getSql(), 0));
			}

			if (selects.size() != 1) {
				log.warn("Did not find a single plain select in " + q.getSql());
				return Arrays.asList(new Subquery(q, q.getSql(), 0));
			}

			List<Subquery> subqueries = new ArrayList<Subquery>(numSubqueries);
			PlainSelect ps = selects.iterator().next();

			Expression oldWhere = ps.getWhere();

			String lt = ">=";
			for (int i = 0; i <= numSubqueries; i++) {

				int keyMin = factTableKeyMin + i
						* ((factTableKeyMax - factTableKeyMin) / numSubqueries);
				int keyMax = Math
						.min(factTableKeyMin
								+ (i + 1)
								* ((factTableKeyMax - factTableKeyMin) / numSubqueries),
								factTableKeyMax);

				if (keyMin == keyMax) {
					break;
				}

				// TODO: add some sort of verification that these do not overlap

				// TODO: also, get the count of rows that match each query.
				// otherwise, it's going hard to recalc averages

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
				// conditions
				subqueries.add(new Subquery(q, ps.toString(), i));
				lt = ">";
			}

			// right, let's rock!
			return subqueries;
		}

	}
}
