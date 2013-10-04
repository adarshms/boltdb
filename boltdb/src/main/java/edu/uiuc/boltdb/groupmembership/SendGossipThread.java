package edu.uiuc.boltdb.groupmembership;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;



public class SendGossipThread implements Runnable
{
	int lossRate;
	//private static org.apache.log4j.Logger log = Logger.getLogger(SendGossipThread.class);
	//@Override
	public void run()
	{
		//System.out.println("GOSSIP THREAD STARTED AT:"+new Date().toString());
		try
		{
			int listSize = GroupMembership.membershipList.size();
			int activeMembersListSize = getActiveMembersCount();
			int gossipGroupSize = (int) Math.ceil(Math.sqrt(activeMembersListSize));
			//System.out.println("Active group size:"+activeMembersListSize + 1);
			Random generator = new Random();
			Object[] keys = GroupMembership.membershipList.keySet().toArray(); 
			int maxTries = 100;
			while(gossipGroupSize > 0 && (maxTries-- > 0))
			{
				MembershipBean mBean = GroupMembership.membershipList.get(keys[generator.nextInt(listSize)]);
				if(mBean.toBeDeleted)
					continue;
				if((mBean.hostname).equals(InetAddress.getLocalHost().getHostName()))
					continue;
				//System.out.println("GOSSIP TO:"+mBean.hostname);
				sendMembershipList(mBean.hostname);
				gossipGroupSize--;
			}
			//System.out.println("DONE GOSSIPPING with tries:"+maxTries);
		}
		catch(Exception e)
		{
			System.out.println("EXCEPTION:In HeartbeatIncrementerThread");
		}
	}
	
	public int getActiveMembersCount()
	{
		int activeMembersCount = 0;
		try
		{
			Collection<MembershipBean> mbeans = GroupMembership.membershipList.values();
			for(MembershipBean mBean : mbeans)
			{
				if(mBean.toBeDeleted)
					continue;
				if((mBean.hostname).equals(InetAddress.getLocalHost().getHostName()))
					continue;
				activeMembersCount++;
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			System.out.println("EXCEPTION:In method getActiveMembersCount in SendGossipThread");
			e.printStackTrace();
		}
		return activeMembersCount;
	}
	
	public void sendMembershipList(String hostname)
	{
		try
		{	// Simulate packet loss
			
			// Generate a random number between 0 and 100. If the generated random number is 
			// in between 0 and the specified lossRate, skip sending the packet.
			Random generator = new Random();
			if(generator.nextInt(100) < lossRate)
				return;
			Thread newThread = new SendMembershipListThread(hostname, 8764);
			newThread.start();
		}
		catch(Exception e)
		{
			System.out.println("EXCEPTION:In method SendMembershipList in SendGossipThread");
		}
	}
}