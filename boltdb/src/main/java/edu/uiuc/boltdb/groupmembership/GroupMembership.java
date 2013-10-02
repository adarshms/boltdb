package edu.uiuc.boltdb.groupmembership;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class GroupMembership {
	//private static org.apache.log4j.Logger log = Logger.getLogger(GroupMembership.class);
	public static ConcurrentHashMap<String,MembershipBean> membershipList = new ConcurrentHashMap<String,MembershipBean>();
	public static String pid = new String();
	
	public static void main(String[] args) throws IOException {
		if(args.length < 1 || !(args[0].equals("-contact"))) {
			System.out.println("Usage: groupmembership -contact <true/false> [-id <id>]");
			System.exit(1);
		}
		
		boolean isContact = false;
		
		if(args[1].equals("true")) isContact = true;
		
		
		if (args.length > 2 && args[2].equals("-id")) pid += args[3] + "-";
		
		pid += InetAddress.getLocalHost().getHostName() + "-" + (new Date().toString());
		GroupMembership.membershipList.putIfAbsent(GroupMembership.pid, new MembershipBean(InetAddress.getLocalHost().getHostName(), 1, System.currentTimeMillis(), false));
		
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		
		if (!isContact) {
			new SendMembershipListThread(prop.getProperty("groupmembership.contact"), 8764).start();
		}
		
		int tFail = Integer.parseInt(prop.getProperty("groupmembership.tfail"));
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(new HeartbeatIncrementerThread(), 0, Integer.parseInt(prop.getProperty("groupmembership.heartbeat.freq")), TimeUnit.MILLISECONDS);
		scheduler.scheduleAtFixedRate(new RefreshMembershipListThread(tFail), 0, Integer.parseInt(prop.getProperty("groupmembership.refreshMembershipList.freq")), TimeUnit.MILLISECONDS);
		
		scheduler.scheduleAtFixedRate(new SendGossipThread(), 0, Integer.parseInt(prop.getProperty("groupmembership.gossip.freq")), TimeUnit.MILLISECONDS);
		Thread receiveGossip = new Thread(new ReceiveGossipThread());
		receiveGossip.start();
	}
}
