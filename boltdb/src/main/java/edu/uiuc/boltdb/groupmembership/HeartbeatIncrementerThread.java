package edu.uiuc.boltdb.groupmembership;

public class HeartbeatIncrementerThread implements Runnable {
	
	@Override
	public void run() {
		
		MembershipBean entry = GroupMembership.membershipList.get(GroupMembership.pid);
		if(entry == null) {
			GroupMembership.membershipList.putIfAbsent(GroupMembership.pid, new MembershipBean(1, System.currentTimeMillis(), false));
		} else {
			entry.hearbeatLastReceived++;
			entry.timeStamp = System.currentTimeMillis();
			GroupMembership.membershipList.put(GroupMembership.pid, entry);
		}
		System.out.println(GroupMembership.membershipList);
	}
}
