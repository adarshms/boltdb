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

public class MergeThread implements Runnable {
	Socket clientSocket = null;
	Map<String,MembershipBean> incomingMembershipList = null;
	
	public  MergeThread(Socket clientSocket) {
		this.clientSocket = clientSocket;
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
	}

	private void getGossipFromClient() throws IOException {
		BufferedReader inFromServer = new BufferedReader(
				new InputStreamReader(clientSocket.getInputStream()));
		String line;
		StringBuilder incomingGossipInJson = new StringBuilder();
		while ((line = inFromServer.readLine()) != null) {
			incomingGossipInJson.append(line);
		}
		
		Gson gson = new GsonBuilder().create();
		Type typeOfMap = new TypeToken<HashMap<String,MembershipBean>>(){}.getType();
		incomingMembershipList = gson.fromJson(incomingGossipInJson.toString(), typeOfMap);
		clientSocket.close();
	}
	
	private void mergeIncomingMembershipList() {
		Iterator<Map.Entry<String, MembershipBean>> iterator = incomingMembershipList.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, MembershipBean> entry = iterator.next();
			String receivedPid = entry.getKey();
			MembershipBean receivedMBean = entry.getValue();
			
			if(receivedMBean.toBeDeleted) continue;
			
			if(GroupMembership.membershipList.contains(receivedPid)) {
				MembershipBean currentMBean = GroupMembership.membershipList.get(receivedPid);
				if (currentMBean.toBeDeleted) continue;
				if(receivedMBean.hearbeatLastReceived > currentMBean.hearbeatLastReceived) {
					currentMBean.hearbeatLastReceived = receivedMBean.hearbeatLastReceived;
					currentMBean.timeStamp = System.currentTimeMillis();
					GroupMembership.membershipList.put(receivedPid, currentMBean);
				}
			} else {
				receivedMBean.timeStamp = System.currentTimeMillis();
				GroupMembership.membershipList.putIfAbsent(receivedPid, receivedMBean);
			}
		}
	}

}
