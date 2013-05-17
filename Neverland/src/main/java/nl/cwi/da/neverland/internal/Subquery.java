package nl.cwi.da.neverland.internal;

public class Subquery extends Query {
	private int slice;
	private long sliceMin;
	private long sliceMax;
	private String factTable;

	private NeverlandNode assignedNode;
	private double timeTakenSecs;
	private double connectTimeSecs;

	private long resultSetSize;

	public Subquery(String sql, int slice) {
		this.sqlQuery = sql;
		this.slice = slice;
	}

	public int getSlice() {
		return slice;
	}

	@Override
	public String toString() {
		return "#" + slice + ": " + sqlQuery;
	}

	public long getSliceMin() {
		return sliceMin;
	}

	public void setSliceMin(long sliceMin) {
		this.sliceMin = sliceMin;
	}

	public long getSliceMax() {
		return sliceMax;
	}

	public void setSliceMax(long sliceMax) {
		this.sliceMax = sliceMax;
	}

	public String getFactTable() {
		return factTable;
	}

	public void setFactTable(String factTable) {
		this.factTable = factTable;
	}

	public NeverlandNode getAssignedNode() {
		return assignedNode;
	}

	public void setAssignedNode(NeverlandNode assignedNode) {
		this.assignedNode = assignedNode;
	}

	public double getTimeTakenSecs() {
		return timeTakenSecs;
	}

	public void setTimeTakenSecs(double timeTakenSecs) {
		this.timeTakenSecs = timeTakenSecs;
	}

	public double getConnectTimeSecs() {
		return connectTimeSecs;
	}

	public void setConnectTimeSecs(double connectTimeSecs) {
		this.connectTimeSecs = connectTimeSecs;
	}

	public long getResultSetSize() {
		return resultSetSize;
	}

	public void setResultSetSize(long resultSetSize) {
		this.resultSetSize = resultSetSize;
	}

	public void setSlice(int slice) {
		this.slice = slice;
	}

}
