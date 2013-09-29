package edu.uiuc.boltdb.groupmembership;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GroupMembership {
	
	public static ConcurrentHashMap<String,MembershipBean> membershipList = new ConcurrentHashMap<String,MembershipBean>();
	public static String pid = null;
	
	public static void main(String[] args) throws IOException {
		membershipList.put("node2", new MembershipBean(InetAddress.getLocalHost().getHostAddress(), 20, System.currentTimeMillis(), false));
		if (args.length != 1) throw new IllegalArgumentException();
		
		pid = args[0]+"-"+InetAddress.getLocalHost().getHostAddress()+"-"+System.currentTimeMillis();
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		/*scheduler.scheduleAtFixedRate(new HeartbeatIncrementerThread(), 0, 1000, TimeUnit.MILLISECONDS);
		scheduler.scheduleAtFixedRate(new RefreshMembershipListThread(), 0, 1000, TimeUnit.MILLISECONDS);
		*/
		scheduler.scheduleAtFixedRate(new SendGossipThread(), 0, 1000, TimeUnit.MILLISECONDS);
		Thread receiveGossip = new Thread(new ReceiveGossipThread());
		receiveGossip.start();
	}
}
