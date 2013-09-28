package edu.uiuc.boltdb.logquerier.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;

import edu.uiuc.boltdb.logquerier.utils.ClientArgs;

/**
 * This class represents the thread which talks to one server.
 * It sends the key regex, value regex and grep options to the server.
 * Once the grep output is available from the server,this thread 
 * saves it in temp file which is printed out by LogQuerierClient class. 
 * If the connection to the server fails, this is also reported to the console.
 * @author ashwin
 *
 */
public class LogQuerierClientThread extends Thread {
	InetAddress address;
	int port;
	ClientArgs clientArgs;
	
	//Contructor to initialize address,port and grep cli options. 
	public LogQuerierClientThread(InetAddress address, int port, ClientArgs args) {
		this.address = address;
		this.port = port;
		this.clientArgs = args;
	}
	

	public void run() {
		Socket connection = null;
		try {
			//connect to the server
			connection = new Socket(address, port);
		} catch (ConnectException ce) {
			//If the server is down,print error on the console and return
			System.out.println();
			System.out.println(address.getHostAddress() + ":" + port
					+ " is not reachable");
			System.out.println();
			return;
		} catch (IOException ioe) {
			System.out.println("IOException:"+ioe.getMessage() +"occurred while connecting to "+address.getHostAddress()+":"+port);
			return;
		}
		try {
		ObjectOutputStream outToServer = new ObjectOutputStream(
				connection.getOutputStream());
		BufferedReader inFromServer = new BufferedReader(
				new InputStreamReader(connection.getInputStream()));

	    //Send the grep options represented in an object to the server
		outToServer.writeObject(clientArgs);
		String line;
		
		//Write the grep output from server to a temp file.
		PrintWriter writer = new PrintWriter("output-"+address.getHostAddress()+"."+port, "UTF-8");
		while ((line = inFromServer.readLine()) != null) {
			writer.println(line);	
		}
		writer.close();
		connection.close();
		} catch(IOException ioe) {
			System.out.println("IOException:"+ioe.getMessage() +"occurred while getting data from "+address.getHostAddress()+":"+port);
		}
	}

	public InetAddress getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	
}

