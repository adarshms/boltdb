package edu.uiuc.boltdb.groupmembership;

public class MembershipBean {
	int hearbeatLastReceived;
	String timeStamp;
	boolean toBeDeleted;
	
	public MembershipBean(int hearbeatLastReceived, String timeStamp,
			boolean toBeDeleted) {
		super();
		this.hearbeatLastReceived = hearbeatLastReceived;
		this.timeStamp = timeStamp;
		this.toBeDeleted = toBeDeleted;
	}
	
	
	
}
