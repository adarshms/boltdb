package edu.uiuc.boltdb.groupmembership;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;
import edu.uiuc.boltdb.groupmembership.beans.UDPBean;

public class MergeThread implements Runnable {
	Map<String,UDPBean> incomingMembershipList = null;
	String receivedJson = new String();
	public  MergeThread(String json) {
		this.receivedJson = json;
	}
	
	@Override
	public void run() {
		try {
			getGossipFromClient();
		} catch (IOException e) {
			System.out.println("Problem receiving gossip");
			return;
		}
		mergeIncomingMembershipList();
		
		//System.out.println("\nAFTER MERGE : "+GroupMembership.membershipList);
	}

	private void getGossipFromClient() throws IOException {
		Gson gson = new GsonBuilder().create();
		Type typeOfMap = new TypeToken<HashMap<String,UDPBean>>(){}.getType();
		incomingMembershipList = gson.fromJson(receivedJson, typeOfMap);
	}
	
	private void mergeIncomingMembershipList() {
		//System.out.println("IN MERGE with incoming mapsize:"+incomingMembershipList.size());
		
		Iterator<Map.Entry<String, UDPBean>> iterator = incomingMembershipList.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, UDPBean> entry = iterator.next();
			String receivedPid = entry.getKey();
			UDPBean receivedMBean = entry.getValue();
			
			//if(receivedMBean.toBeDeleted) continue;
			if(GroupMembership.membershipList.containsKey(receivedPid)) {
				MembershipBean currentMBean = GroupMembership.membershipList.get(receivedPid);
				if (currentMBean.toBeDeleted) continue;
				if(receivedMBean.hearbeatLastReceived > currentMBean.hearbeatLastReceived) {
					//System.out.println("UPDATED HEARTBEAT");
					currentMBean.hearbeatLastReceived = receivedMBean.hearbeatLastReceived;
					currentMBean.timeStamp = System.currentTimeMillis();
					GroupMembership.membershipList.put(receivedPid, currentMBean);
					//System.out.println("CURRENTMBEAN : " + currentMBean);
				}
			} else {
				String receivedHost = receivedPid.split(GroupMembership.pidDelimiter)[0];
				MembershipBean mBean = new MembershipBean(receivedHost, receivedMBean.hearbeatLastReceived, System.currentTimeMillis(), false);
				MembershipBean returnVal = GroupMembership.membershipList.putIfAbsent(receivedPid, mBean);
				if (returnVal == null) {
					System.out.println("JOINED : " + receivedPid);
				}
			}
		}
	}

}
