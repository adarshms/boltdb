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
			//String clientArgsStr = "keyRegExp : " + clientArgs.getKeyRegExp() + " valueRegExp : " + clientArgs.getValRegExp();
			//System.out.println("INFO : Arguements from client -> " + clientArgsStr);
			//String regExp = getRegExp(clientArgs.getKeyRegExp(), clientArgs.getValRegExp());
			String logFileName = "machine." + this.server.getServerId() + ".log";
			String command = getGrepCommand(clientArgs, logFileName);
			System.out.println("Command : " + command);
			//System.out.println("INFO : RegExp built from client arguments : " + regExp);
			//String command = "awk 'BEGIN {FS=\" - - \"} " + regExp + " {print $0}' " + logFile;
			//String cmd = "grep "
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
	
	/*public String getRegExp(String keyRegExp, String valRegExp)
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
	}*/
	
	public String getGrepCommand(ClientArgs clientArgs, String logFileName)
	{
		String keyRegExp = clientArgs.getKeyRegExp();
		String valRegExp = clientArgs.getValRegExp();
		String options = clientArgs.getOptionsString();
		String command = "";

		if(!keyRegExp.isEmpty() && !valRegExp.isEmpty())
		{
			//command = "grep" + options + " -E '(" + keyRegExp + ".* - - )' " + logFileName + " | grep" + options + " -E '( - - .*" + valRegExp + ")'";
			command = "grep" + options + " -E '(" + keyRegExp + ".* - - .*" + valRegExp + ")' " + logFileName;
			return command;
		}
		if(!keyRegExp.isEmpty())
		{
			command = "grep" + options + " -E '(" + keyRegExp + ".* - - )' " + logFileName;
			return command;
		}
		if(!valRegExp.isEmpty())
		{
			command = "grep" + options + " -E '( - - .*" + valRegExp + ")' " + logFileName;
			return command;
		}
		return command;
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