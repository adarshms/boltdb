package edu.uiuc.boltdb.groupmembership;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;

public class RefreshMembershipListThread implements Runnable {
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	// TODO take tFail from property file
	public RefreshMembershipListThread(int tFail) {
		this.tFail = tFail;
	}
	private int tFail;

	@Override
	public void run() {
		Iterator<Map.Entry<String, MembershipBean>> iterator = GroupMembership.membershipList
				.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, MembershipBean> entry = iterator.next();
			MembershipBean membershipBean = entry.getValue();
			if (membershipBean.toBeDeleted) {
				GroupMembership.membershipList.remove(entry.getKey());
			} else if (System.currentTimeMillis() - membershipBean.timeStamp >= tFail * 1000) {
				membershipBean.toBeDeleted = true;
				System.out.println("CRASHED : " + entry.getKey() +" at " + new Date().toString());
				log.info("CRASHED - - - " + entry.getKey());
			}
		}
		//System.out.println("REFRESH MEMBERSHIP THREAD : "+GroupMembership.membershipList);
	}
}
