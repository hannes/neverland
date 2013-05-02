package nl.cwi.da.neverland.internal;

public class NeverlandNode {
	private String jdbc;
	private String sessionId;

	public NeverlandNode(String jdbc, String sessionId) {
		this.jdbc = jdbc;
		this.sessionId = sessionId;
	}

	public String getId() {
		return sessionId;
	}

	public String getJdbc() {
		return jdbc;
	}

}
