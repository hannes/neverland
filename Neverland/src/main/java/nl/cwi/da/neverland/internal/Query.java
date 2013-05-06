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
}
