package edu.uiuc.boltdb.logquerier.server;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;

/**
 * This class represents the "server" component of the distribued log querier. It creates a server socket and
 * listens to requests from clients. When a request is received, it spawns a thread (an instance of 
 * LogQuerierServerThread) and continues to listen.
 * 
 * Input: Takes two command line inputs - <serverId> <port>
 * <serverId> is the unique id of the machine on which the server is running
 * <port> is the port on which the server should listen to requests
 * 
 * @author adarshms
 */

public class LogQuerierServer 
{
	private ServerSocket serverSocket;
	private int serverId;
	
	/**
	 * Constructor to create the server socket and start listening
	 * @param serverId
	 * @param port
	 */
	
	public LogQuerierServer(int serverId, int port)
	{  
		this.serverId = serverId;
		try
	    {  
			System.out.println("INFO : Binding to port " + port + ", please wait  ...");
			serverSocket = new ServerSocket(port);  
	        System.out.println("INFO : Server started : " + serverSocket);
	        startListening();
	    }
	    catch(IOException ioe)
	    {  
	    	System.out.println(ioe); 
	    }
	}
	
	public int getServerId()
	{
		return serverId;
	}
	
	
	/**
	 *  This method runs and infinite loop and keeps listening for client requests. When it receives a
	 *  client request, it accepts the connection and passes the client socket object to the spawnTaskThread
	 *  method. 
	 */
	public void startListening()
	{
		try 
		{
			while(true) 
			{             
				spawnTaskThread(serverSocket.accept());
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
	}
	
	/**
	 * This method receives a clientSocket object after the server has accepted a connection from a client. It 
	 * spawns a LogQuerierServerThread and passes the clientSocket object to the thread for it to handle the 
	 * further communication.
	 * @param clientSocket
	 */
	public void spawnTaskThread(Socket clientSocket)
	{
		LogQuerierServerThread taskThread = new LogQuerierServerThread(this, clientSocket);
		taskThread.start();
	}
	
	
	/**
	 * The main method that creates an instance of LogQuerierServer by accepting the serverId and port
	 * from the command line.
	 * @param args
	 */
	public static void main(String[] args)
	{
		LogQuerierServer logQuerierServer = null;
		if (args.length != 2)
			System.out.println("Usage: java -cp boltdb-0.0.1-SNAPSHOT.jar edu.uiuc.boltdb.logquerier.server.LogQuerierServer <server_id> <port_number>");
	    else
	    {
	    	int serverId = 1;
	    	int port = 6789;
    		try 
    		{
    			serverId = Integer.parseInt(args[0]);
				port = Integer.parseInt(args[1]);
			} 
    		catch (NumberFormatException e) 
    		{
				e.printStackTrace();
			}
	    	logQuerierServer = new LogQuerierServer(serverId, port);
	    }
	}
}
