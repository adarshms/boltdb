package edu.uiuc.boltdb.groupmembership;

public class MembershipBean {
	String ipaddress;
	long hearbeatLastReceived;
	long timeStamp;
	boolean toBeDeleted;
	
	public MembershipBean(String ipaddress, long hearbeatLastReceived, long timeStamp,
			boolean toBeDeleted) {
		super();
		this.ipaddress = ipaddress;
		this.hearbeatLastReceived = hearbeatLastReceived;
		this.timeStamp = timeStamp;
		this.toBeDeleted = toBeDeleted;
	}
	
	@Override
	public String toString() {
		return new String("["+hearbeatLastReceived+" "+timeStamp+" "+toBeDeleted+"]");
	}
	
}
