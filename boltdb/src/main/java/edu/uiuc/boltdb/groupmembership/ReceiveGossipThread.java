package edu.uiuc.boltdb.groupmembership;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * This thread listens for gossip messages. On receiving a message,it spawns a MergeThread to handle it. 
 * @author ashwin
 *
 */
public class ReceiveGossipThread implements Runnable 
{
	private DatagramSocket serverSocket;
	private int gossipPort = 8764;
	
	public ReceiveGossipThread() throws IOException 
	{
		serverSocket = new DatagramSocket(gossipPort);
	}
	
	public void run() 
	{
		while(true) 
		{
			byte[] receiveData = new byte[2048];
			
			//Create a datagram receive packet
			DatagramPacket receive = new DatagramPacket(receiveData, receiveData.length);
			try 
			{
				serverSocket.receive(receive);
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			GroupMembership.bandwidth += receive.getLength() + 8;
			//System.out.println("Packet received:"+(receive.getLength() + 8));
			String receivedJson = new String(receive.getData());
			String sentHost = receive.getAddress().getHostName();
			MergeThread mergeThread = new MergeThread(sentHost,receivedJson.trim());
			new Thread(mergeThread).start();			
		}
	}
}
