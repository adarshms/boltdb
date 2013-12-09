package edu.uiuc.boltdb.groupmembership.beans;

/**
 * This bean just stores the heartbeat for a particular pid. This is used to reduce the size of the gossip
 * messages.
 */

public class UDPBean {
	public long hearbeatLastReceived;
	public long hashValue;
	public UDPBean(long hearbeatLastReceived, long hashValue) {
		super();
		this.hearbeatLastReceived = hearbeatLastReceived;
		this.hashValue = hashValue;
	}
	
	
}
