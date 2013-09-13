package edu.uiuc.boltdb.logquerier.server;

import java.net.*;
import java.io.*;

import edu.uiuc.boltdb.logquerier.utils.ClientArgs;

/**
 * This class represents the task performed by the server upon each client request. The instance of 
 * LogQuerierServerThread is constructed with a clientSocket object. The thread receives arguments 
 * from the client over the clientSocket. It performs the grep operation using these arguments on the log file 
 * named machine.<serverId>.log, where serverId is the serverId field of the LogQuerierServer object that spawned
 * this thread. It then writes back the output of the grep command to the clientSocket.
 *  
 * @author adarshms
 */

public class LogQuerierServerThread extends Thread
{  
	private LogQuerierServer server = null;
	private Socket clientSocket = null;
	private ObjectInputStream readFromClient = null;
	private DataOutputStream writeToClient = null;

	/**
	 * Constructor to create the thread
	 * @param server
	 * @param clientSocket
	 */
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
			String logFileName = "machine." + this.server.getServerId() + ".log";
			String command = getGrepCommand(clientArgs, logFileName);
			System.out.println("Command : " + command);
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
	
	public String getGrepCommand(ClientArgs clientArgs, String logFileName)
	{
		String keyRegExp = clientArgs.getKeyRegExp();
		String valRegExp = clientArgs.getValRegExp();
		String options = clientArgs.getOptionsString();
		String command = "";

		if(!keyRegExp.isEmpty() && !valRegExp.isEmpty())
		{
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