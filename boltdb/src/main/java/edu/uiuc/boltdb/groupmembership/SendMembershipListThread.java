package edu.uiuc.boltdb.groupmembership;

import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import edu.uiuc.boltdb.groupmembership.beans.*;

/**
 * This thread sends the MembershipList to the specified hostname. The thread is constructed with a 
 * hostname and a port. It serializes the MembershipList it a JSON string and sends it to the specified host
 * as a UDP Datagram packet.
 */

public class SendMembershipListThread extends Thread 
{
	String hostname;
	int port;
	
	public SendMembershipListThread(String hostname, int port) 
	{
		this.hostname = hostname;
		this.port = port;
	}

	public void run() 
	{
		try
		{
			// Create a client datagram socket
			DatagramSocket clientSocket = new DatagramSocket();
			
			// Uses Google gson for serialization
			Gson gson = new GsonBuilder().create();
			Type typeOfHashMap = new TypeToken<HashMap<String, UDPBean>>(){}.getType();
			
			// Send only the entries that are not marked "toBeDeleted"
			Iterator<Map.Entry<String, MembershipBean>> iterator = GroupMembership.membershipList.entrySet().iterator();
			HashMap<String,UDPBean> listToSend = new HashMap<String,UDPBean>();
			while (iterator.hasNext()) 
			{
				Map.Entry<String, MembershipBean> entry = iterator.next();
				if(entry.getValue().toBeDeleted)
					continue;
				listToSend.put(entry.getKey(), new UDPBean(entry.getValue().hearbeatLastReceived));
			}
			String json = gson.toJson(listToSend, typeOfHashMap);
			byte[] jsonBytes = json.getBytes();
			DatagramPacket dataPacket = new DatagramPacket(jsonBytes, jsonBytes.length, InetAddress.getByName(hostname), port);
			clientSocket.send(dataPacket);
			clientSocket.close();
		}
		catch(Exception e)
		{
			System.out.println("EXCEPTION:In SendMembershipListThread");
		}
	}	
}
