package edu.uiuc.boltdb.logquerier.server;

import java.net.*;
import java.io.*;

import edu.uiuc.boltdb.logquerier.utils.ClientArgs;

public class LogQuerierServerThread extends Thread
{  
	private LogQuerierServer server = null;
	private Socket clientSocket = null;
	private ObjectInputStream readFromClient = null;
	private DataOutputStream writeToClient = null;

	public LogQuerierServerThread(LogQuerierServer server, Socket clientSocket)
	{
		this.server = server;
		this.clientSocket = clientSocket;  
	}
	
	public void run()
	{  
		try 
		{
			InetAddress clientInetAddress = clientSocket.getInetAddress();
			System.out.println("INFO : Connected to client at : " + clientInetAddress);
			readFromClient = new ObjectInputStream(clientSocket.getInputStream());
			ClientArgs clientArgs = (ClientArgs)readFromClient.readObject();
			String regExp = getRegExp(clientArgs.getKeyRegExp(), clientArgs.getValRegExp());
			System.out.println("INFO : RegExp built from client arguments : " + regExp);
			String logFile = "machine." + this.server.getServerId() + ".log";
			
			String command = "awk 'BEGIN {FS=\" - - \"} " + regExp + " {print $0}' " + logFile;
			
			Runtime rt = Runtime.getRuntime();
			Process ps = rt.exec(new String[] {"/bin/sh", "-c", command});
			BufferedReader is = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			String line;
			writeToClient = new DataOutputStream(clientSocket.getOutputStream());
			while ((line = is.readLine()) != null) 
			{
			    writeToClient.writeBytes(line + "\n");
			}
			close();
		} 
		catch (IOException ioe) 
		{
			ioe.printStackTrace();
		}
		catch (ClassNotFoundException cnfe) 
		{
			cnfe.printStackTrace();
		} 
	}
	
	public String getRegExp(String keyRegExp, String valRegExp)
	{
		String regExp = "";
		if(!keyRegExp.isEmpty() && !valRegExp.isEmpty())
		{
			regExp = " || ";
		}
		if(!keyRegExp.isEmpty())
		{
			regExp = "$1~/" + keyRegExp + "/" + regExp;
		}
		if(!valRegExp.isEmpty())
		{
			regExp = regExp + "$2~/" + valRegExp + "/";
		}
		return regExp;
	}
   
	public void close() throws IOException
    {  
		if(clientSocket != null)
			clientSocket.close();
		if(readFromClient != null)
			readFromClient.close();
		if(writeToClient != null)
			writeToClient.close();
    }
}