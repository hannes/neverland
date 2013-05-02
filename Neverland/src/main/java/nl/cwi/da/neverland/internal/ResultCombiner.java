package nl.cwi.da.neverland.internal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.sun.rowset.CachedRowSetImpl;

public abstract class ResultCombiner {
	public abstract ResultSet combine(Query q, List<ResultSet> sets);

	public static class ConcatResultCombiner extends ResultCombiner {
		@Override
		public ResultSet combine(Query q, List<ResultSet> sets) {
			CachedRowSetImpl crs = null;
			try {
				crs = new CachedRowSetImpl();
				for (ResultSet rs : sets) {
					crs.populate(rs);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return crs;
		}
	}
}
