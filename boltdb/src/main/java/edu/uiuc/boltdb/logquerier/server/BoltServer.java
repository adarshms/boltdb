package edu.uiuc.boltdb.logquerier.server;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;

public class BoltServer 
{
	private ServerSocket serverSocket;
	
	public BoltServer(int port)
	{  
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
		TaskThread taskThread = new TaskThread(clientSocket);
		taskThread.start();
	}
	
	public static void main(String[] args)
	{
		BoltServer boltServer = null;
		if (args.length != 1)
			System.out.println("Usage: java -cp boltdb-0.0.1-SNAPSHOT.jar edu.uiuc.boltdb.logquerier.server.BoltServer <port_number>");
	    else
	    {
	    	int port = 6789;
    		try 
    		{
				port = Integer.parseInt(args[0]);
			} 
    		catch (NumberFormatException e) 
    		{
				e.printStackTrace();
			}
	    	boltServer = new BoltServer(port);
	    }
	}
}
