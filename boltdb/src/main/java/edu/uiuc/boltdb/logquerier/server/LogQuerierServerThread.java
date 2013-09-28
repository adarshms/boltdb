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
	
	/**
	 * The run method does the following tasks -
	 * 1) Reads the arguments from the client
	 * 2) Constructs the grep command by calling getGrepCommand method
	 * 3) Executes the grep command and streams the data back to the client
	 */
	public void run()
	{  
		try 
		{
			InetAddress clientInetAddress = clientSocket.getInetAddress();
			System.out.println("INFO : Connected to client at : " + clientInetAddress);
			
			// Read arguments from client
			readFromClient = new ObjectInputStream(clientSocket.getInputStream());
			ClientArgs clientArgs = (ClientArgs)readFromClient.readObject();
			
			String logFileName = "machine." + this.server.getServerId() + ".log";
			
			// Get the command to execute by calling getGrepCommand and passing it the arguments
			String command = getGrepCommand(clientArgs, logFileName);
			System.out.println("Command : " + command);
			
			// Execute the command
			Runtime rt = Runtime.getRuntime();
			Process ps = rt.exec(new String[] {"/bin/sh", "-c", command});
			
			// Read the output of the command and stream it back to the client
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
	
	/**
	 * The getGrepCommand method takes the client arguments and the logFileName, 
	 * constructs the grep command from these arguments and returns the grep command string.
	 * @param clientArgs
	 * @param logFileName
	 * @return
	 */
	public String getGrepCommand(ClientArgs clientArgs, String logFileName)
	{
		String keyRegExp = clientArgs.getKeyRegExp();
		String valRegExp = clientArgs.getValRegExp();
		String options = clientArgs.getOptionsString();
		String command = "";
		
		// Handle corner cases of "$" apperaring at the end of keyRegExp and "^" appearing at the end 
		// of valRegExp
		if(!(keyRegExp.isEmpty()))
		{
			if(keyRegExp.endsWith("$"))
			{
				keyRegExp = keyRegExp.substring(0, keyRegExp.length()-1);
			}
			else
				keyRegExp += ".*";
		}
		if(!(valRegExp.isEmpty()))
		{
			if(valRegExp.startsWith("^"))
			{
				valRegExp = valRegExp.substring(1);
			}
			else
				valRegExp = ".*" + valRegExp;
		}
		
		// Build the command using the keyRegExp and valRegExp
		if(!(keyRegExp.isEmpty()) && !(valRegExp.isEmpty()))
		{
			command = "grep" + options + " -E '(" + keyRegExp + ":" + valRegExp + ")' " + logFileName;
			return command;
		}
		if(!keyRegExp.isEmpty())
		{
			command = "grep" + options + " -E '(" + keyRegExp + ":)' " + logFileName;
			return command;
		}
		if(!valRegExp.isEmpty())
		{
			command = "grep" + options + " -E '(:" + valRegExp + ")' " + logFileName;
			return command;
		}
		return command;
	}
   
	/**
	 * This method closes client socket, the read and write streams opened on the client socket
	 * @throws IOException
	 */
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