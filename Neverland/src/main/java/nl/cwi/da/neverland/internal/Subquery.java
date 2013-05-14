package nl.cwi.da.neverland.internal;

public class Subquery extends Query {
	private Query parent;
	private int slice;

	public Subquery(Query parent, String sql, int slice) {
		this.parent = parent;
		this.sqlQuery = sql;
		this.slice = slice;
	}

	public Query getParent() {
		return parent;
	}

	public int getSlice() {
		return slice;
	}

	@Override
	public String toString() {
		return "#" + slice + ": " + sqlQuery;
	}
}
