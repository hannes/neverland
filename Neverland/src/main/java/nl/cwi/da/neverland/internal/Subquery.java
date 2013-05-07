package nl.cwi.da.neverland.internal;

public class Subquery extends Query {
	private Query parent;
	private int id;

	public Subquery(Query parent, String sql, int id) {
		this.parent = parent;
		this.sqlQuery = sql;
		this.id = id;
	}

	public Query getParent() {
		return parent;
	}

	public int getId() {
		return id;
	}

	@Override
	public String toString() {
		return "#" + id + ": " + sqlQuery;
	}
}
