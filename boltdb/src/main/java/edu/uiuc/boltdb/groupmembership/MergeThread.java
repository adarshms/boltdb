package edu.uiuc.boltdb.groupmembership;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.BoltDBServer;
import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;
import edu.uiuc.boltdb.groupmembership.beans.UDPBean;

/**
 * This class is responsible for unmarshalling gossip messages received from other nodes and
 * merging it with its membership list.  
 *
 */

public class MergeThread implements Runnable 
{
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	Map<String,UDPBean> incomingMembershipList = null;
	String receivedJson = new String();
	String sentHost;
	
	public  MergeThread(String sentHost, String json) 
	{
		this.sentHost = sentHost;
		this.receivedJson = json;
	}
	
	public void run() 
	{
		try 
		{
			getGossipFromClient();
		} 
		catch (IOException e) 
		{
			System.out.println("Problem receiving gossip");
			return;
		}
		try {
			mergeIncomingMembershipList();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		//System.out.println("\nAFTER MERGE : "+GroupMembership.membershipList);
	}

	/**
	 * Unmarshall gossip message and converts it into a map datastructure called 'incomingMembershipList'.  
	 * @throws IOException
	 */
	private void getGossipFromClient() throws IOException 
	{
		Gson gson = new GsonBuilder().create();
		Type typeOfMap = new TypeToken<HashMap<String,UDPBean>>(){}.getType();
		incomingMembershipList = gson.fromJson(receivedJson, typeOfMap);
	}
	
	/**
	 * Merge the incoming membership list into the current node's membership list
	 * @throws UnknownHostException 
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws MalformedURLException 
	 * @throws NoSuchAlgorithmException 
	 */
	private void mergeIncomingMembershipList() throws UnknownHostException, MalformedURLException, RemoteException, NotBoundException, NoSuchAlgorithmException 
	{
		Iterator<Map.Entry<String, UDPBean>> iterator = incomingMembershipList.entrySet().iterator();
		//Iterate over each entry of incoming membershiplist
		while (iterator.hasNext()) 
		{
			Map.Entry<String, UDPBean> entry = iterator.next();
			String receivedPid = entry.getKey();
			UDPBean receivedMBean = entry.getValue();
			
			//Check if pid present in this entry is also present in our membership list.
			if(GroupMembership.membershipList.containsKey(receivedPid)) 
			{
				MembershipBean currentMBean = GroupMembership.membershipList.get(receivedPid);
				
				//If this entry represents the current node and the heartbeat is less than zero(which means this node has voluntary left),
				//then don't do anything
				if(currentMBean.hearbeatLastReceived <= 0 && receivedPid.equals(GroupMembership.pid)) continue;
				
				//AVOID PING PONG: If the node is marked toBeDeleted then dont do anything. This would avoid ping pong effect. 
				if (currentMBean.toBeDeleted && !receivedPid.startsWith(sentHost)) continue;
				
				//VOLUNTARILY LEAVE : If the incoming entry has heartbeat less than zero,then log 'VOLUNTARILY LEFT' message.
				//Also update current node's membership list
				if(receivedMBean.hearbeatLastReceived <= 0 && currentMBean.hearbeatLastReceived > 0) {
					System.out.println("VOLUNTARILY LEFT : " + receivedPid+ " at "+(new Date()).toString());
					log.info("VOLUNTARILY LEFT - - - " + receivedPid);
					currentMBean.hearbeatLastReceived = -1;
					currentMBean.timeStamp = System.currentTimeMillis();
					currentMBean.toBeDeleted = false;
					GroupMembership.membershipList.put(receivedPid, currentMBean);
					continue;
				} else if (receivedMBean.hearbeatLastReceived <= 0 || currentMBean.hearbeatLastReceived <= 0) continue;
				
				//If the incoming entry's heartbeat is greater than current node's membership list,then update the list.
                if(receivedMBean.hearbeatLastReceived > currentMBean.hearbeatLastReceived) 
                {
                        currentMBean.hearbeatLastReceived = receivedMBean.hearbeatLastReceived;
                        currentMBean.timeStamp = System.currentTimeMillis();
                        if(currentMBean.toBeDeleted) {
                                System.out.println("JOINED : " + receivedPid);
                                currentMBean.toBeDeleted = false;
                        }
                        GroupMembership.membershipList.put(receivedPid, currentMBean);
                }
			}
			else if(!GroupMembership.membershipList.containsKey(receivedPid) && receivedMBean.hearbeatLastReceived > 0) 
			{
				//JOIN : If the incoming entry is not in our membership list then it means a new node has joined.
				//if(receivedMBean.hearbeatLastReceived <= 0) continue;
				String receivedHost = receivedPid.split(GroupMembership.pidDelimiter)[0];
				MembershipBean mBean = new MembershipBean(receivedHost, receivedMBean.hearbeatLastReceived, System.currentTimeMillis(), receivedMBean.hashValue, false);
				MembershipBean returnVal = GroupMembership.membershipList.putIfAbsent(receivedPid, mBean);
				if (returnVal == null) 
				{
					//System.out.println("JOINED : " + receivedPid+" at "+(new Date()).toString());
					log.info("JOINED - - - " + receivedPid);
					//Get the successor of newly joined node
					boolean amISuccessor = amITheSuccesorOf(receivedPid);
					
					if(amISuccessor) {
						//If you are the successor,move keys accordingly. 
						moveKeys(receivedHost,mBean.hashValue);
					}
				}
			}
		}
	}
	
	/**
	 * Check if I am the successor of the newly joined node.
	 * @param receivedPid
	 * @return
	 * @throws UnknownHostException
	 */
	private boolean amITheSuccesorOf(String receivedPid) throws UnknownHostException {
		long hashOfNewlyJoinedNode = GroupMembership.membershipList.get(receivedPid).hashValue;
		if(GroupMembership.membershipList.get(GroupMembership.getSuccessorNode(hashOfNewlyJoinedNode)).hostname.equals(InetAddress.getLocalHost().getHostName())) return true;
		return false;
	}
	
	/**
	 * Move keys from yourself to the newly joined node just like in the chord paper
	 * @param targetHost
	 * @param hashOfNewJoinedNode
	 * @throws MalformedURLException
	 * @throws RemoteException
	 * @throws NotBoundException
	 * @throws NoSuchAlgorithmException
	 */
	private void moveKeys(String targetHost, long hashOfNewJoinedNode) throws MalformedURLException, RemoteException, NotBoundException, NoSuchAlgorithmException {
		//get the rmiserver handle from the rmi registry
		BoltDBProtocol targetRMIServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
		Iterator<Entry<Long,String>> itr = BoltDBServer.KVStore.entrySet().iterator();
		long myHash = GroupMembership.membershipList.get(GroupMembership.pid).hashValue;
		
		// Get the rmiserver handle for successor's successor from the rmi registry
		String succSuccessorHost = GroupMembership.membershipList.get(GroupMembership.getKthSuccessorNode(myHash, 2)).hostname;
		BoltDBProtocol succSuccRMIServer = (BoltDBProtocol) Naming.lookup("rmi://" + succSuccessorHost + "/KVStore");
		while(itr.hasNext()) {
			Entry<Long,String> entry = itr.next();
			long hashOfKey = GroupMembership.computeHash(entry.getKey().toString());
			//If hash of current server is greater than hash of newly joined server
			//then move all the keys greater than hash of current server
			//and less than newly joined server.
			if (myHash > hashOfNewJoinedNode) {
				if ( hashOfKey > myHash || hashOfKey <= hashOfNewJoinedNode) {
					targetRMIServer.insert(entry.getKey(), entry.getValue(),false);
					//BoltDBServer.KVStore.remove(entry.getKey());
				} 
			}
			//If hash of current server is less than hash of newly joined server
			//then move all the keys in between the two servers hashes.
			else {
				if ( hashOfKey > myHash && hashOfKey <= hashOfNewJoinedNode) {
				targetRMIServer.insert(entry.getKey(), entry.getValue(),false);
				BoltDBServer.KVStore.remove(entry.getKey());
				} 
			}
			
			// Delete this key in successor's successor
			succSuccRMIServer.delete(entry.getKey(), false);
		}
	}
}
