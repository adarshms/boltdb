package edu.uiuc.boltdb.groupmembership;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;
import edu.uiuc.boltdb.groupmembership.beans.UDPBean;

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
	
	//@Override
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
		mergeIncomingMembershipList();
		//System.out.println("\nAFTER MERGE : "+GroupMembership.membershipList);
	}

	private void getGossipFromClient() throws IOException 
	{
		Gson gson = new GsonBuilder().create();
		Type typeOfMap = new TypeToken<HashMap<String,UDPBean>>(){}.getType();
		incomingMembershipList = gson.fromJson(receivedJson, typeOfMap);
	}
	
	private void mergeIncomingMembershipList() 
	{
		Iterator<Map.Entry<String, UDPBean>> iterator = incomingMembershipList.entrySet().iterator();
		while (iterator.hasNext()) 
		{
			Map.Entry<String, UDPBean> entry = iterator.next();
			String receivedPid = entry.getKey();
			UDPBean receivedMBean = entry.getValue();
			
			//if(receivedMBean.toBeDeleted) continue;
			if(GroupMembership.membershipList.containsKey(receivedPid)) 
			{
				MembershipBean currentMBean = GroupMembership.membershipList.get(receivedPid);
				if(currentMBean.hearbeatLastReceived <= 0 && receivedPid.equals(GroupMembership.pid)) continue;
				
				if (currentMBean.toBeDeleted && !receivedPid.startsWith(sentHost)) continue;
				
				if(receivedMBean.hearbeatLastReceived <= 0 && currentMBean.hearbeatLastReceived > 0) {
					System.out.println("VOLUNTARILY LEFT : " + receivedPid+ " at "+(new Date()).toString());
					log.info("VOLUNTARILY LEFT - - - " + receivedPid);
					currentMBean.hearbeatLastReceived = -1;
					currentMBean.timeStamp = System.currentTimeMillis();
					currentMBean.toBeDeleted = false;
					GroupMembership.membershipList.put(receivedPid, currentMBean);
					continue;
				} else if (receivedMBean.hearbeatLastReceived <= 0 || currentMBean.hearbeatLastReceived <= 0) continue;
				
				if(receivedMBean.hearbeatLastReceived > currentMBean.hearbeatLastReceived) 
				{
					//System.out.println("UPDATED HEARTBEAT");
					currentMBean.hearbeatLastReceived = receivedMBean.hearbeatLastReceived;
					currentMBean.timeStamp = System.currentTimeMillis();
					if(currentMBean.toBeDeleted) {
						System.out.println("JOINED : " + receivedPid);
						currentMBean.toBeDeleted = false;
					}
					GroupMembership.membershipList.put(receivedPid, currentMBean);
					//System.out.println("CURRENTMBEAN : " + currentMBean);
				}
			} 
			else 
			{
				if(receivedMBean.hearbeatLastReceived <= 0) continue;
				String receivedHost = receivedPid.split(GroupMembership.pidDelimiter)[0];
				MembershipBean mBean = new MembershipBean(receivedHost, receivedMBean.hearbeatLastReceived, System.currentTimeMillis(), false);
				MembershipBean returnVal = GroupMembership.membershipList.putIfAbsent(receivedPid, mBean);
				if (returnVal == null) 
				{
					System.out.println("JOINED : " + receivedPid+" at "+(new Date()).toString());
					//System.out.println("^^Rcved heartb:"+receivedMBean.hearbeatLastReceived);
					log.info("JOINED - - - " + receivedPid);
				}
			}
		}
	}
}
