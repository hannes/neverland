package nl.cwi.da.neverland.internal;

import java.io.Serializable;

public class NeverlandNode implements Serializable {
	@Override
	public String toString() {
		return "NeverlandNode [jdbcUrl=" + jdbcUrl + ", jdbcUser=" + jdbcUser
				+ ", jdbcPass=" + jdbcPass + ", sessionId=" + sessionId
				+ ", load=" + load + "]";
	}

	private static final long serialVersionUID = 1L;

	private String jdbcUrl;
	private String jdbcUser;
	private String jdbcPass;
	private String sessionId;
	private double load;

	public NeverlandNode(String jdbcUrl, String jdbcUser, String jdbcPass,
			String sessionId, double load) {
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPass = jdbcPass;
		this.sessionId = sessionId;
		this.load = load;
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

	public double getLoad() {
		return load;
	}

	public void setLoad(double systemLoadAverage) {
		this.load = systemLoadAverage;
	}

}
