package edu.uiuc.boltdb.logquerier.server;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;

public class LogQuerierServer 
{
	private ServerSocket serverSocket;
	private int serverId;
	
	public LogQuerierServer(int id, int port)
	{  
		serverId = id;
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
	
	public void spawnTaskThread(Socket clientSocket)
	{
		LogQuerierServerThread taskThread = new LogQuerierServerThread(this, clientSocket);
		taskThread.start();
	}
	
	public static void main(String[] args)
	{
		LogQuerierServer boltServer = null;
		if (args.length != 2)
			System.out.println("Usage: java -cp boltdb-0.0.1-SNAPSHOT.jar edu.uiuc.boltdb.logquerier.server.BoltServer <server_id> <port_number>");
	    else
	    {
	    	int id = 1;
	    	int port = 6789;
    		try 
    		{
    			id = Integer.parseInt(args[0]);
				port = Integer.parseInt(args[1]);
			} 
    		catch (NumberFormatException e) 
    		{
				e.printStackTrace();
			}
	    	boltServer = new LogQuerierServer(id, port);
	    }
	}
}
