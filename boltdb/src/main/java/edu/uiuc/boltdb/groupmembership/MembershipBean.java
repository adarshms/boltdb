package edu.uiuc.boltdb.groupmembership;

public class MembershipBean {
	String hostname;
	long hearbeatLastReceived;
	long timeStamp;
	boolean toBeDeleted;
	
	public MembershipBean(String ipaddress, long hearbeatLastReceived, long timeStamp,
			boolean toBeDeleted) {
		super();
		this.hostname = ipaddress;
		this.hearbeatLastReceived = hearbeatLastReceived;
		this.timeStamp = timeStamp;
		this.toBeDeleted = toBeDeleted;
	}
	
	@Override
	public String toString() {
		return new String("[" + hostname + " " +hearbeatLastReceived+" "+timeStamp+" "+toBeDeleted+"]");
	}
	
}
