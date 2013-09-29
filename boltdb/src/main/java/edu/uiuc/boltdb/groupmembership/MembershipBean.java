package edu.uiuc.boltdb.groupmembership;

public class MembershipBean {
	long hearbeatLastReceived;
	long timeStamp;
	boolean toBeDeleted;
	
	public MembershipBean(long hearbeatLastReceived, long timeStamp,
			boolean toBeDeleted) {
		super();
		this.hearbeatLastReceived = hearbeatLastReceived;
		this.timeStamp = timeStamp;
		this.toBeDeleted = toBeDeleted;
	}
	
	@Override
	public String toString() {
		return new String("["+hearbeatLastReceived+" "+timeStamp+" "+toBeDeleted+"]");
	}
	
}
