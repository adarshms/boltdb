package edu.uiuc.boltdb.groupmembership;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GroupMembership {
	
	public static ConcurrentHashMap<String,MembershipBean> membershipList = new ConcurrentHashMap<String,MembershipBean>();
	public static String pid = null;
	
	public static void main(String[] args) throws UnknownHostException {
		membershipList.put("node2", new MembershipBean(20, System.currentTimeMillis(), false));
		if (args.length != 1) throw new IllegalArgumentException();
		
		pid = args[0]+"-"+InetAddress.getLocalHost().getHostAddress()+"-"+System.currentTimeMillis();
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(new HeartbeatIncrementerThread(), 0, 1000, TimeUnit.MILLISECONDS);
		scheduler.scheduleAtFixedRate(new RefreshMembershipListThread(), 0, 1000, TimeUnit.MILLISECONDS);
	}
}
