package edu.uiuc.boltdb.groupmembership;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class ReceiveGossipThread implements Runnable 
{
	private DatagramSocket serverSocket;
	private int gossipPort = 8764;
	
	public ReceiveGossipThread() throws IOException 
	{
		serverSocket = new DatagramSocket(gossipPort);
	}
	
	//@Override
	public void run() 
	{
		while(true) 
		{
			byte[] receiveData = new byte[2048];
		
			DatagramPacket receive = new DatagramPacket(receiveData, receiveData.length);
			try 
			{
				serverSocket.receive(receive);
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			String receivedJson = new String(receive.getData());
			String sentHost = receive.getAddress().getHostName();
			//System.out.println(receivedJson.trim());
			MergeThread mergeThread = new MergeThread(sentHost,receivedJson.trim());
			new Thread(mergeThread).start();			
		}
	}
}
