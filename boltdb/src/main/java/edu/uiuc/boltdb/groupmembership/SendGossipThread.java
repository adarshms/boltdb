package edu.uiuc.boltdb.groupmembership;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Random;

import org.apache.log4j.Logger;

public class SendGossipThread implements Runnable
{
	//private static org.apache.log4j.Logger log = Logger.getLogger(SendGossipThread.class);
	@Override
	public void run()
	{
		try
		{
			int listSize = GroupMembership.membershipList.size();
			int activeMembersListSize = getActiveMembersCount();
			int gossipGroupSize = (int) Math.sqrt(activeMembersListSize);
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
				sendMembershipList(mBean.hostname);
				gossipGroupSize--;
			}
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
		{
			Thread newThread = new SendMembershipListThread(hostname, 8764);
			newThread.start();
		}
		catch(Exception e)
		{
			System.out.println("EXCEPTION:In method SendMembershipList in SendGossipThread");
		}
	}
}