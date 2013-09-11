package edu.uiuc.boltdb.logquerier.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;

import edu.uiuc.boltdb.logquerier.utils.ClientArgs;

public class LogQuerierClientThread extends Thread {
	InetAddress address;
	int port;
	ClientArgs clientArgs;
	
	public LogQuerierClientThread(InetAddress address, int port, ClientArgs args) {
		this.address = address;
		this.port = port;
		this.clientArgs = args;
	}
	
	public void run() {
		Socket connection = null;
		try {
			connection = new Socket(address, port);
		} catch (ConnectException ce) {
			System.out.println(address.getHostAddress() + ":" + port
					+ " is not reachable");
			System.out.println();
		} catch (IOException ioe) {
			System.out.println("IOException:"+ioe.getMessage() +"occurred while connecting to "+address.getHostAddress()+":"+port);
		}
		try {
		ObjectOutputStream outToServer = new ObjectOutputStream(
				connection.getOutputStream());
		BufferedReader inFromServer = new BufferedReader(
				new InputStreamReader(connection.getInputStream()));

	
		outToServer.writeObject(clientArgs);
		String line;
		//System.out.println("Logs from " + address.getHostAddress()+":"+port);
		//System.out
		//		.println("-------------------------------------------------");
		while ((line = inFromServer.readLine()) != null)
			System.out.println("["+address.getHostAddress()+":"+port+"]"+line);

		System.out.println();
		System.out.println();
		connection.close();
		} catch(IOException ioe) {
			System.out.println("IOException:"+ioe.getMessage() +"occurred while getting data from "+address.getHostAddress()+":"+port);
		}
	}

}

