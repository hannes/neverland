package nl.cwi.da.neverland.internal;


public class NeverlandException extends Exception {
	private static final long serialVersionUID = 1L;

	public NeverlandException(String string) {
		super(string);
	}

	public NeverlandException(Exception e) {
		super(e);
	}

	public NeverlandException(String reason, Exception e) {
		super(reason, e);
	}

}
