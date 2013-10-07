package edu.uiuc.boltdb.groupmembership;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;

/**
 * This class is responsible for checking every 'groupmembership.refreshMembershipList.freq' seconds
 * if any entry in the membership list has timed out. The timeout value ,also called 'tFail' is picked 
 * up from the property file. An entry is marked toBeDeleted if its timed-out after tFail and then
 * removed from the map after another tFail seconds. 
 *
 */
public class RefreshMembershipListThread implements Runnable 
{
	private static org.apache.log4j.Logger log = Logger.getRootLogger();

	public RefreshMembershipListThread(int tFail) 
	{
		this.tFail = tFail;
	}
	private int tFail;

	public void run() 
	{
		Iterator<Map.Entry<String, MembershipBean>> iterator = GroupMembership.membershipList
				.entrySet().iterator();
		while (iterator.hasNext()) 
		{
			Map.Entry<String, MembershipBean> entry = iterator.next();
			MembershipBean membershipBean = entry.getValue();
			
			if(membershipBean.hearbeatLastReceived <= 0 && entry.getKey().equals(GroupMembership.pid)) {
				continue;
			} 
			
			if (membershipBean.toBeDeleted) 
			{
				GroupMembership.membershipList.remove(entry.getKey());
			} 
			else if (System.currentTimeMillis() - membershipBean.timeStamp >= tFail * 1000) 
			{
				membershipBean.toBeDeleted = true;
				if (membershipBean.hearbeatLastReceived > 0) {
					System.out.println("CRASHED : " + entry.getKey() +" at " + new Date().toString());
					log.info("CRASHED - - - " + entry.getKey());
				}
			}
		}
		//System.out.println("REFRESH MEMBERSHIP THREAD : "+GroupMembership.membershipList);
	}
}
