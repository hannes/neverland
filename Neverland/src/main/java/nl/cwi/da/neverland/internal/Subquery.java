package nl.cwi.da.neverland.internal;

public class Subquery extends Query {
	private Query parent;

	public Subquery(Query parent) {
		this.parent = parent;
	}
}
