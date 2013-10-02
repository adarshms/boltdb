package edu.uiuc.boltdb.groupmembership;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;

public class ReceiveGossipThread implements Runnable {

	private DatagramSocket serverSocket;
	private int gossipPort = 8764;
	

	public ReceiveGossipThread() throws IOException {
		serverSocket = new DatagramSocket(gossipPort);
	}
	@Override
	public void run() {
		while(true) {
		byte[] receiveData = new byte[2048];
		
		DatagramPacket receive = new DatagramPacket(receiveData, receiveData.length);
		try {
			serverSocket.receive(receive);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String receivedJson = new String(receive.getData());
		//System.out.println(receivedJson.trim());
		MergeThread mergeThread = new MergeThread(receivedJson.trim());
		new Thread(mergeThread).start();			
			
	}
		
	}
	
	
}
