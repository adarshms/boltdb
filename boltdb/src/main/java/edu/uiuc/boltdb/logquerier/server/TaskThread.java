package edu.uiuc.boltdb.logquerier.server;

import java.net.*;
import java.io.*;

import edu.uiuc.boltdb.logquerier.utils.ClientArgs;

public class TaskThread extends Thread
{  
	private Socket clientSocket = null;
	private ObjectInputStream readFromClient = null;
	private DataOutputStream writeToClient = null;

	public TaskThread(Socket clientSocket)
	{  
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
			String command = "grep -E " + regExp + " machine.i.log";
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
			regExp = " | ";
		}
		if(!keyRegExp.isEmpty())
		{
			regExp = keyRegExp + regExp;
		}
		if(!valRegExp.isEmpty())
		{
			regExp = regExp + valRegExp;
		}
		regExp = "'(" + regExp + ")'";
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