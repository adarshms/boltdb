package edu.uiuc.boltdb.groupmembership;

import java.net.InetAddress;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;

public class HeartbeatIncrementerThread implements Runnable {
	//private static org.apache.log4j.Logger log = Logger.getLogger(HeartbeatIncrementerThread.class);
	@Override
	public void run() {
		try{
		
			MembershipBean entry = GroupMembership.membershipList.get(GroupMembership.pid);
			if(entry == null) {
				GroupMembership.membershipList.putIfAbsent(GroupMembership.pid, new MembershipBean(InetAddress.getLocalHost().getHostAddress(), 1, System.currentTimeMillis(), false));
			} else {
				entry.hearbeatLastReceived++;
				entry.timeStamp = System.currentTimeMillis();
				GroupMembership.membershipList.put(GroupMembership.pid, entry);
			}
		}catch(Exception e){
			System.out.println("EXCEPTION:In HeartbeatIncrementerThread");
		}
	}
}
