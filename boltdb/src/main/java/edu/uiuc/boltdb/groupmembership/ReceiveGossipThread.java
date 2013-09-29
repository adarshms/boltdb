package edu.uiuc.boltdb.groupmembership;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ReceiveGossipThread implements Runnable {

	private ServerSocket serverSocket;
	private int gossipPort = 8190;
	public ReceiveGossipThread() throws IOException {
		serverSocket = new ServerSocket(gossipPort);
	}
	@Override
	public void run() {
			
		while (true) {
			try {
				Socket clientSocket = serverSocket.accept();
				MergeThread mergeThread = new MergeThread(clientSocket);
				//TODO think of failure scenarios here
				new Thread(mergeThread).start();
			} catch (IOException e) {
				System.out.println("Problem accepting client connection.");
			}
			
			
			
		}
		
	}
	
	
}
