package edu.uiuc.boltdb.groupmembership.beans;

/**
 * This bean encapsulates the hostname, timeStamp and the toBeDeleted flag of for each machine. The instance of 
 * this class goes in as the value in the MembershipList
 */
public class MembershipBean extends UDPBean {
	public String hostname;
	public long timeStamp;
	public boolean toBeDeleted;
	
	public MembershipBean(String ipaddress, long hearbeatLastReceived, long timeStamp,
			long hashValue, boolean toBeDeleted) {
		super(hearbeatLastReceived, hashValue);
		this.hostname = ipaddress;
		this.timeStamp = timeStamp;
		this.hashValue = hashValue;
		this.toBeDeleted = toBeDeleted;
	}
	
	@Override
	public String toString() {
		return new String("[" + hostname + " " +hearbeatLastReceived+" "+timeStamp+" "+toBeDeleted+ " " + hashValue + "]");
	}
	
}
