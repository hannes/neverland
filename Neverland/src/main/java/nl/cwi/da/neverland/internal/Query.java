package nl.cwi.da.neverland.internal;

public class Query {
	protected String sqlQuery;

	public Query() {
	}

	public Query(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public String getSql() {
		return sqlQuery;
	}
}
