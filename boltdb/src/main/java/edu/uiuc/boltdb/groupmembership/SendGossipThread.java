package edu.uiuc.boltdb.groupmembership;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;

/**
 * This thread runs every "groupmembership.gossip.freq" seconds. It gets all the active entries(n) in the
 * MembershipList(excluding itself) and randomly selects sqrt(n) machines from the list and sends gossip messages
 * to these machines by spawning the SendMembershipListThread for each machine. This thread also simulates 
 * message losses for experimental purposes
 */

public class SendGossipThread implements Runnable
{
	int lossRate;
	
	public SendGossipThread(int lossRate) 
	{
		this.lossRate = lossRate;
	}

	//@Override
	public void run()
	{
		try
		{
			int listSize = GroupMembership.membershipList.size();
			int activeMembersListSize = getActiveMembersCount();
			int gossipGroupSize = (int) Math.ceil((Math.sqrt(activeMembersListSize)));
			Random generator = new Random();
			Object[] keys = GroupMembership.membershipList.keySet().toArray(); 
			
			// Try for a maximum of 100 times
			int maxTries = 100;
			while(gossipGroupSize > 0 && (maxTries-- > 0))
			{
				MembershipBean mBean = GroupMembership.membershipList.get(keys[generator.nextInt(listSize)]);
				if(mBean.toBeDeleted)
					continue;
				if((mBean.hostname).equals(InetAddress.getLocalHost().getHostName()))
					continue;
				sendMembershipList(mBean.hostname);
				gossipGroupSize--;
			}
		}
		catch(Exception e)
		{
			System.out.println("EXCEPTION:In HeartbeatIncrementerThread");
		}
	}

	// Method to get the active members in the MembershipList (excluding the local entry)
	public int getActiveMembersCount()
	{
		int activeMembersCount = 0;
		try
		{
			Collection<MembershipBean> mbeans = GroupMembership.membershipList.values();
			for(MembershipBean mBean : mbeans)
			{
				// Do not count if the entry is marked "toBeDeleted"
				if(mBean.toBeDeleted)
					continue;
				// Do not count if the entry is the local machine's entry
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
	
	
	// This method spawns a SendMembershipListThread to send a gossip message to each of the selected machines
	public void sendMembershipList(String hostname)
	{
		try
		{	
			// Simulate packet loss
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