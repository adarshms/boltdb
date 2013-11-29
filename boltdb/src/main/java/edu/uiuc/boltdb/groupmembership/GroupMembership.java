package edu.uiuc.boltdb.groupmembership;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.BoltDBServer;
import edu.uiuc.boltdb.groupmembership.beans.*;

/**
 * This class is the entry point for Distributed Group Membership. It takes two
 * arguments - 1) -contact <true/false> : true if its the contact machine,false
 * otherwise 2) -id <machine-id> : this is an argument to identify a machine and
 * will be used to name its log file
 * 
 * Data structures : The membership list is maintained inside a
 * ConcurrentHashMap<String,MembershipBean>. There are two advantages of doing
 * this : 1. Thread Safety : Both read/write on the membership list is thread
 * safe. 2. Fast : only a portion of the map is locked while writing into it and
 * not the whole map. This means other READ threads can continue accessing the
 * map while another writes to it.
 * 
 * Parameters : This class needs 6 parameters to function and all of these come
 * from a property file boltdb.prop. The parameters are contact hostname,gossip
 * frequency, heartbeat increment frequency, tfail, refresh membership list
 * frequency, lossrate(for 4th credit).
 * 
 * The class initializes and starts the following threads : 1.
 * ReceiveGossipThread 2. HeartbeatIncrementerThread 3.
 * RefreshMembershipListThread 4. SendGossipThread
 * 
 * More detils about each of the thread can be found in the javadoc of its
 * class.
 * 
 * @author ashwin(ashanka2)
 * 
 */
public class GroupMembership implements Runnable {
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	public static ConcurrentHashMap<String, MembershipBean> membershipList = new ConcurrentHashMap<String, MembershipBean>();
	public static String pid = new String();
	public static String pidDelimiter = "--";
	public static long bandwidth = 0;
	public static int replicationFactor = 1;
	private String[] args;

	public GroupMembership(String args[]) {
		this.args = args;
	}

	/**
	 * Initialize the logger. Set the name of the log file with the machineid
	 * passed as parameter.
	 * 
	 * @param serverId
	 */
	public static void initializeLogger(String serverId) {
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger");
		fa.setFile(serverId + ".log");
		fa.setLayout(new PatternLayout("%m%n"));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		log.addAppender(fa);
	}

	public void run() {
		// Command line parsing
		if (args.length < 1 || !(args[0].equals("-contact"))) {
			System.out
					.println("Usage: groupmembership -contact <true/false> [-id <id>]");
			System.exit(1);
		}

		boolean isContact = false;
		if (args[1].equals("true"))
			isContact = true;
		try {
			pid += InetAddress.getLocalHost().getHostName()
					+ GroupMembership.pidDelimiter + (new Date().toString());
			if (args.length > 2 && args[2].equals("-id")) {
				pid += "-" + args[3];
				initializeLogger(args[3]);
			}
			
			//Compute the hashvalue of yourself(server)
			long hashValue = computeHash(pid);
			
			// Insert the current machine into the membership list with
			// heartbeat=1. This single entry is going to be sent to the contact
			// node for joining the cluster.
			GroupMembership.membershipList.putIfAbsent(
					GroupMembership.pid,
					new MembershipBean(
							InetAddress.getLocalHost().getHostName(), 1, System
									.currentTimeMillis(), hashValue, false));

			// Load all the parameters needed from the property file.
			Properties prop = new Properties();
			FileInputStream fis = new FileInputStream("./boltdb.prop");
			prop.load(fis);
			fis.close();
			
			// Set the replicaton factor from the properties file
			replicationFactor = Integer.parseInt(prop.getProperty("groupmembership.rfactor"));

			// Start the thread that listens to gossip messages.
			Thread receiveGossip = new Thread(new ReceiveGossipThread());
			receiveGossip.start();

			// JOINING : This is the JOIN part . So,if this node is not the
			// contact machine,then try to connect to the contact machine
			// and send your membership list which contains just one entry ie
			// current machine's details.
			if (!isContact) {
				int maxTries = 10;
				while (maxTries-- > 0) {
					new SendMembershipListThread(
							prop.getProperty("groupmembership.contact"), 8764)
							.start();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (GroupMembership.membershipList.size() > 1)
						break;
				}
			}

			// Get the tFail from property file
			int tFail = Integer.parseInt(prop
					.getProperty("groupmembership.tfail"));

			// ScheduledExecutorService is used to schedule all the threads
			// mentioned in the class javadoc with frequency mentioned in
			// property file
			ScheduledExecutorService scheduler = Executors
					.newSingleThreadScheduledExecutor();
			scheduler.scheduleAtFixedRate(new HeartbeatIncrementerThread(), 0,
					Integer.parseInt(prop
							.getProperty("groupmembership.heartbeat.freq")),
					TimeUnit.MILLISECONDS);
			scheduler
					.scheduleAtFixedRate(
							new RefreshMembershipListThread(tFail),
							0,
							Integer.parseInt(prop
									.getProperty("groupmembership.refreshMembershipList.freq")),
							TimeUnit.MILLISECONDS);
			scheduler.scheduleAtFixedRate(
					new SendGossipThread(Integer.parseInt(prop
							.getProperty("groupmembership.lossrate"))), 0,
					Integer.parseInt(prop
							.getProperty("groupmembership.gossip.freq")),
					TimeUnit.MILLISECONDS);
			// scheduler.scheduleAtFixedRate(new LogBandwidthThread(), 0, 60000,
			// TimeUnit.MILLISECONDS);

			// VOLUTARY LEAVE : This is the code for voluntary leave part.
			// Basically we wait for user to input the string "leave".
			// Once the user enters "leave",all the threads are stopped. The
			// heartbeat of the current node is set to -1 and
			// one last gossip happens.
			// Please note that we don't send the last message to everyone in
			// the list.
			BufferedReader bufferRead = new BufferedReader(
					new InputStreamReader(System.in));
			while (true) {
				// Read user's input
				System.out.print("boltdb-server>");
				String commandString = bufferRead.readLine();
				if (commandString.equals(""))
					continue;
				if (commandString.equals("leave")) {
					receiveGossip.stop();
					scheduler.shutdownNow();
					scheduler.awaitTermination(100, TimeUnit.MILLISECONDS);
					//Move your keys to successor
					long myHash = GroupMembership.membershipList.get(GroupMembership.pid).hashValue;
					String successor = getSuccessorNode(myHash);
					BoltDBProtocol successorRMIServer = (BoltDBProtocol) Naming.lookup("rmi://" + successor + "/KVStore");
					Iterator<Entry<Long,String>> itr = BoltDBServer.KVStore.entrySet().iterator();
					
					while(itr.hasNext()) {
						Entry<Long,String> entry = itr.next();
						successorRMIServer.insert(entry.getKey(), entry.getValue(), false);
					}
					BoltDBServer.KVStore.clear();
					
					MembershipBean mBean = membershipList.get(pid);
					mBean.hearbeatLastReceived = -1;
					mBean.timeStamp = System.currentTimeMillis();
					membershipList.put(pid, mBean);
					Thread gossipOneLastTime = new Thread(new SendGossipThread(
							0));
					gossipOneLastTime.start();
					break;
				}
				/*
				 * The show command prints the current membershipList entries and the current KVStore
				 * entries on the console.
				 */
				else if(commandString.equals("show")) {
					System.out.println("-------------------------------------------------");
					System.out.println("Membership List : ");
					System.out.println("-------------------------------------------------");
					for (Map.Entry<String, MembershipBean> entry : membershipList.entrySet())
					{
						System.out.println(entry);
					}
					System.out.println("-------------------------------------------------");
					System.out.println();
					System.out.println("-------------------------------------------------");
					System.out.println("Key Value Store : ");
					System.out.println("-------------------------------------------------");
					for (Map.Entry<Long, String> entry : BoltDBServer.KVStore.entrySet())
					{
					    System.out.println(entry.getKey() + " ---> " + entry.getValue() + "   |   Hash Value of Key - " + computeHash((new Long(entry.getKey())).toString()));
					}
					System.out.println("-------------------------------------------------");
					System.out.println();
				}
				else if(commandString.equals("ring")) {
					System.out.println("-------------------------------------------------");
					System.out.println("Node Ring : ");
					int noOfNodes = membershipList.size();
					long node = computeHash(this.pid);
					while(noOfNodes-- > 0) {
						System.out.print(node + " --> ");
						node = computeHash(getSuccessorNode(node));
					}
					System.out.println("looparound");
					System.out.println("-------------------------------------------------");
					System.out.println();
				}
				else {
					StringTokenizer stk = new StringTokenizer(commandString);
					String checkCommand = stk.nextToken();
					if(checkCommand.equals("checksucc"))
					{
						long thisNode = Long.parseLong(stk.nextToken());
						long failedNode = Long.parseLong(stk.nextToken());
						System.out.println("inSuccReReplicationSeg output --> " + inSuccReReplicationSeg(thisNode, failedNode));
					}
					else if(checkCommand.equals("checkpred"))
					{
						long thisNode = Long.parseLong(stk.nextToken());
						long failedNode = Long.parseLong(stk.nextToken());
						System.out.println("inPredReReplicationSeg output --> " + inPredReReplicationSeg(thisNode, failedNode));
					}
					else if(checkCommand.equals("succ"))
					{
						long thisNode = Long.parseLong(stk.nextToken());
						System.out.println("Successor of " + thisNode + " --> " + computeHash(getSuccessorNode(thisNode)));
					}
					else if(checkCommand.equals("pred"))
					{
						long thisNode = Long.parseLong(stk.nextToken());
						System.out.println("Predecessor of " + thisNode + " --> " + computeHash(getPredecessorNode(thisNode)));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * Computes the MD5 hash of the pid,transforms it into an integer
	 * and hashes it in the range 0-1 million.
	 * @param pid
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	public static long computeHash(String pid) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		BigInteger bigInt = new BigInteger(1, md.digest(pid.getBytes()));
		return Math.abs(bigInt.longValue()) % 1000001L;
	}
	
	/**
	 * Returns the node which is the successor of a key.
	 * @param keyHash
	 * @return
	 */
	public static String getSuccessorNode(long aNode) {
		Iterator<Entry<String,MembershipBean>> itr = GroupMembership.membershipList.entrySet().iterator();
		//Set the minimum clockwise distance to be  maximum possible value
		long minClockwiseDistance = 1000000L;
		String successorNode = new String();
		while(itr.hasNext()) {
			Entry<String,MembershipBean> entry = itr.next();
			//Ignore if the entry is yourself(Server)
			if(entry.getValue().hashValue == aNode) continue;
			long hashCurrent = entry.getValue().hashValue;
			//compute the clockwise distance
			long clockWiseDistance = aNode > hashCurrent ? 1000000l - (aNode - hashCurrent) : hashCurrent - aNode;
			//Update minimum clockwise distance if required
			if(minClockwiseDistance > clockWiseDistance) {
				minClockwiseDistance = clockWiseDistance;
				successorNode = entry.getKey();
			}
		}
		return successorNode;
	}
	
	
	/**
	 * Returns the node which is the predecessor of a key.
	 * @param keyHash
	 * @return
	 */
	public static String getPredecessorNode(long aNode) {
		Iterator<Entry<String,MembershipBean>> itr = GroupMembership.membershipList.entrySet().iterator();
		//Set the maximum clockwise distance to be  minimum possible value
		long maxClockwiseDistance = 0L;
		String predecessorNode = new String();
		while(itr.hasNext()) {
			Entry<String,MembershipBean> entry = itr.next();
			//Ignore if the entry is yourself(Server)
			if(entry.getValue().hashValue == aNode) continue;
			long hashCurrent = entry.getValue().hashValue;
			//compute the clockwise distance
			long clockWiseDistance = aNode > hashCurrent ? 1000000L - (aNode - hashCurrent) : hashCurrent - aNode;
			//Update minimum clockwise distance if required
			if(maxClockwiseDistance < clockWiseDistance) {
				maxClockwiseDistance = clockWiseDistance;
				predecessorNode = entry.getKey();
			}
		}
		return predecessorNode;
	}
	
	public static int inSuccReReplicationSeg(long thisNode, long failedNode) throws NoSuchAlgorithmException
	{
		int k = replicationFactor;
		while(k-- > 0) {
			if((failedNode=computeHash(getSuccessorNode(failedNode))) == thisNode)
				return (replicationFactor - k);
		}
		return -1;
	}
	
	public static int inPredReReplicationSeg(long thisNode, long failedNode) throws NoSuchAlgorithmException
	{
		int k = replicationFactor;
		while(k-- > 0) {
			if((failedNode=computeHash(getPredecessorNode(failedNode))) == thisNode)
				return (replicationFactor - k);
		}
		return -1;
	}
}