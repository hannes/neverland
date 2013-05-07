package nl.cwi.da.neverland.internal;

public class NeverlandNode {
	private String jdbcUrl;
	private String jdbcUser;
	private String jdbcPass;
	private String sessionId;

	public NeverlandNode(String jdbcUrl, String jdbcUser, String jdbcPass,
			String sessionId) {
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPass = jdbcPass;
		this.sessionId = sessionId;
	}

	public String getId() {
		return sessionId;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public String getJdbcUser() {
		return jdbcUser;
	}

	public String getJdbcPass() {
		return jdbcPass;
	}

}
