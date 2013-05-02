package nl.cwi.da.neverland.internal;

public class Subquery extends Query {
	private Query parent;

	public Subquery(Query parent, String sql) {
		this.parent = parent;
		this.sqlQuery = sql;
	}

	public Query getParent() {
		return parent;
	}
}
