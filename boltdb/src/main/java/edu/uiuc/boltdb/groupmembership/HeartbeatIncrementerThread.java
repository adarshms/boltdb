package edu.uiuc.boltdb.groupmembership;

import java.net.InetAddress;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;

/**
 * This class increments the hearbeat of the current node.
 * @author ashwin
 *
 */
public class HeartbeatIncrementerThread implements Runnable 
{
	//@Override
	public void run() 
	{
		try
		{
			MembershipBean entry = GroupMembership.membershipList.get(GroupMembership.pid);
			if(entry == null) 
			{
				GroupMembership.membershipList.putIfAbsent(GroupMembership.pid, new MembershipBean(InetAddress.getLocalHost().getHostAddress(), 1, System.currentTimeMillis(), false));
			} 
			else 
			{
				//Dont update the heartbeat if its less than zero which means the node has voluntarily left.
				if(entry.hearbeatLastReceived <= 0) return;
				//Increment the heartbeat
				entry.hearbeatLastReceived++;
				//Update the timestamp
				entry.timeStamp = System.currentTimeMillis();
				GroupMembership.membershipList.put(GroupMembership.pid, entry);
			}
		}
		catch(Exception e)
		{
			System.out.println("EXCEPTION:In HeartbeatIncrementerThread");
		}
	}
}
