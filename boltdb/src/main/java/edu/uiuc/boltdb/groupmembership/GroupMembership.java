package edu.uiuc.boltdb.groupmembership;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import edu.uiuc.boltdb.groupmembership.beans.*;

/**This class is the entry point for Distributed Group Membership.
 * It takes two arguments - 
 * 1) -contact <true/false> : true if its the contact machine,false otherwise
 * 2) -id <machine-id> : this is an argument to identify a machine and will be used to name its log file
 * 
 * Data structures :
 * The membership list is maintained inside a ConcurrentHashMap<String,MembershipBean>.
 * There are two advantages of doing this : 
 * 	1. Thread Safety : Both read/write on the membership list is thread safe.
 *  2. Fast : only a portion of the map is locked while writing into it and not the whole map.
 *  		  This means other READ threads can continue accessing the map while another writes to it.
 *  
 * Parameters : 
 * This class needs 6 parameters to function and all of these come from a property file boltdb.prop.
 * The parameters are contact hostname,gossip frequency, heartbeat increment frequency, tfail, refresh membership list frequency, lossrate(for 4th credit). 
 * 
 * The class initializes and starts the following threads :
 * 1. ReceiveGossipThread
 * 2. HeartbeatIncrementerThread
 * 3. RefreshMembershipListThread
 * 4. SendGossipThread
 * 
 * More detils about each of the thread can be found in the javadoc of its class.
 * @author ashwin(ashanka2)
 *
 */
public class GroupMembership 
{
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	public static ConcurrentHashMap<String,MembershipBean> membershipList = new ConcurrentHashMap<String,MembershipBean>();
	public static String pid = new String();
	public static String pidDelimiter = "--";
	public static long bandwidth = 0;
	
	/**
	 * Initialize the logger. Set the name of the log file with the machineid passed as parameter.
	 * @param serverId
	 */
	public static void initializeLogger(String serverId)
	{
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger");
		fa.setFile(serverId + ".log");
		fa.setLayout(new PatternLayout("%m%n"));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		log.addAppender(fa);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException 
	{
		//Command line parsing
		if(args.length < 1 || !(args[0].equals("-contact"))) 
		{
			System.out.println("Usage: groupmembership -contact <true/false> [-id <id>]");
			System.exit(1);
		}
		
		boolean isContact = false;
		if(args[1].equals("true")) 
			isContact = true;
		
		pid += InetAddress.getLocalHost().getHostName() + GroupMembership.pidDelimiter + (new Date().toString());
		if (args.length > 2 && args[2].equals("-id"))
		{
			pid += "-" + args[3];
			initializeLogger(args[3]);
		}
		
		//Insert the current machine into the membership list with heartbeat=1. This single entry is going to be sent to the contact node for joining the cluster. 
		GroupMembership.membershipList.putIfAbsent(GroupMembership.pid, new MembershipBean(InetAddress.getLocalHost().getHostName(), 1, System.currentTimeMillis(), false));
		
		//Load all the paramters needed from the property file.
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		
		//Start the thread that listens to gossip messages.
		Thread receiveGossip = new Thread(new ReceiveGossipThread());
		receiveGossip.start();
		
		//JOINING : This is the JOIN part . So,if this node is not the contact machine,then try to connect to the contact machine
		//and send your membership list which contains just one entry ie current machine's details.
		if (!isContact) 
		{
			int maxTries = 10;
			while(maxTries-- > 0)
			{
				new SendMembershipListThread(prop.getProperty("groupmembership.contact"), 8764).start();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(GroupMembership.membershipList.size() > 1) break;
			}
		}
		
		//Get the tFail from property file
		int tFail = Integer.parseInt(prop.getProperty("groupmembership.tfail"));
		
		//ScheduledExecutorService is used to schedule all the threads mentioned in the class javadoc with frequency mentioned in property file
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(new HeartbeatIncrementerThread(), 0, Integer.parseInt(prop.getProperty("groupmembership.heartbeat.freq")), TimeUnit.MILLISECONDS);
		scheduler.scheduleAtFixedRate(new RefreshMembershipListThread(tFail), 0, Integer.parseInt(prop.getProperty("groupmembership.refreshMembershipList.freq")), TimeUnit.MILLISECONDS);
		scheduler.scheduleAtFixedRate(new SendGossipThread(Integer.parseInt(prop.getProperty("groupmembership.lossrate"))), 0, Integer.parseInt(prop.getProperty("groupmembership.gossip.freq")), TimeUnit.MILLISECONDS);
		//scheduler.scheduleAtFixedRate(new LogBandwidthThread(), 0, 60000, TimeUnit.MILLISECONDS);

		//VOLUTARY LEAVE : This is the code for voluntary leave part. Basically we wait for user to input the string "leave".
		//Once the user enters "leave",all the threads are stopped. The heartbeat of the current node is set to -1 and 
		//one last gossip happens.
		//Please note that we don't send the last message to everyone in the list. 
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
		while(true) {
			String s = bufferRead.readLine();
			if(s.equals("leave")) {
				receiveGossip.stop();
				scheduler.shutdownNow();
				scheduler.awaitTermination(100, TimeUnit.MILLISECONDS);
				MembershipBean mBean = membershipList.get(pid);
				mBean.hearbeatLastReceived = -1;
				mBean.timeStamp = System.currentTimeMillis();
				membershipList.put(pid, mBean);
				Thread gossipOneLastTime = new Thread(new SendGossipThread(0));
				gossipOneLastTime.start();
				break;
			}
		}

	}
}
