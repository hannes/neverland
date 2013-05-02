package nl.cwi.da.neverland.internal;

import java.util.Arrays;
import java.util.List;

public abstract class Rewriter {
	public abstract List<Subquery> rewrite(Query q);

	public static class StupidRewriter extends Rewriter {
		@Override
		public List<Subquery> rewrite(Query q) {
			return Arrays.asList(new Subquery(q, q.getSql()));
		}
	}
}
