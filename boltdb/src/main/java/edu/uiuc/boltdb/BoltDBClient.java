package edu.uiuc.boltdb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * This class represents the client component of the distributed key value store. On start up it creates 
 * a boltdb-client> shell where the user can type in the insert, update, lookup and delete queries. 
 * The BoltDBClient class creates a reference to a remote BoltDBServer object in the member variable boltDBServer.
 * All the operations (insert, lookup, update and delete) are performed on this remote object reference.  
 * 
 * @author Adarsh
 *
 */

public class BoltDBClient {
	
	private BoltDBProtocol boltDBServer = null;
	
	public BoltDBClient(String rmiString) throws MalformedURLException, RemoteException, NotBoundException {
		if(System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		boltDBServer = (BoltDBProtocol) Naming.lookup("rmi://" + rmiString + "/KVStore");
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws NotBoundException 
	 */
	public static void main(String[] args) {
		try {
			Properties prop = new Properties();
			FileInputStream fis = new FileInputStream("./boltdb.prop");
			prop.load(fis);
			fis.close();
			String rmiString = prop.getProperty("boltdb.kvstore.server");
			BoltDBClient boltDBClient = new BoltDBClient(rmiString);
			boltDBClient.runClientShell();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	// Method to simulate a boltdb-client shell
	private void runClientShell() throws IOException {
		// Simulate a unix shell
		 String commandString = "";
		 BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
		
		 // Break out with Ctrl+C
		while (true) {
		  // Read user's input
		  System.out.print("boltdb-client>");
		  commandString = console.readLine();

		  // If the user entered a return, just loop again
		  if (commandString.equals(""))
			continue;
		  handleCommand(commandString);
		}
	}
	
	// This method handles the commands entered by the user in the boltdb-client shell
	private void handleCommand(String commandString) {
		try {
			StringTokenizer stk = new StringTokenizer(commandString);
			if(stk.countTokens() < 2) {
				printUsage(0);
				return;
			}
			
			// The Command Type
			String commandType = stk.nextToken();
			
			if(commandType.equals("insert")) {
				String keyStr = stk.nextToken();
				long key = parseKey(keyStr);
				if(key == -1) return;
				String value = "";
				if(stk.hasMoreTokens()) {
					while(stk.hasMoreTokens())
						value += stk.nextToken() + " ";
				}
				else {
					printUsage(1);
					return;
				}
				value = value.trim();
				// Perform Insert Operation on the remote server object
				boltDBServer.insert(key, value, true);
			} else if(commandType.equals("update")) {
				String keyStr = stk.nextToken();
				long key = parseKey(keyStr);
				if(key == -1) return;
				String value = "";
				if(stk.hasMoreTokens()) {
					while(stk.hasMoreTokens())
						value += stk.nextToken() + " ";
				}
				else {
					printUsage(2);
					return;
				}
				value = value.trim();
				// Perform Update Operation on the remote server object
				boltDBServer.update(key, value, true);
			} else if(commandType.equals("lookup")) {
				String keyStr = stk.nextToken();
				long key = parseKey(keyStr);
				if(key == -1) return;
				// Perform LookUp Operation on the remote server object
				String value = (String)boltDBServer.lookup(key, true);
				System.out.println("Look Up Result : " + value);
			} else if(commandType.equals("delete")) {
				String keyStr = stk.nextToken();
				long key = parseKey(keyStr);
				if(key == -1) return;
				// Perform Delete Operation on the remote server object
				boltDBServer.delete(key, true);
				System.out.println("Key Value Pair Deleted");
			} else {
				printUsage(0);
				return;
			}
		} catch(RemoteException re) {
			if(re.getCause().getCause() != null)
				System.out.println(re.getCause().getCause().getMessage());
			else
				System.out.println(re.getCause().getMessage());
		}
	}

	// Method to parse the key entered by the user to long, and to validate the key 
	private long parseKey(String keyStr) {
		long key;
		try {
			key = Long.parseLong(keyStr);
			if(key < 0L || key > 1000000L) {
				System.out.println("Invalid Key Range. Key should be in the Range 0 - 1000000");
				return -1L;
			}
			return key;
		} catch(NumberFormatException nfe) {
			System.out.println("Invalid Key : Please enter an Integer value for key");
			return -1L;
		}
	}
	
	// Method to print the usage of BoltDBClient
	private void printUsage(int type) {
		System.out.println("Invalid Command");
		System.out.println("Usage : ");
		switch(type) {
		case 1: System.out.println("insert <key> <value>");
				break;
		case 2: System.out.println("update <key> <value>");
				break;
		case 3: System.out.println("lookukp <key>");
				break;
		case 4: System.out.println("delete <key>");
				break;
		default:System.out.println("insert <key> <value>");
				System.out.println("update <key> <value>");
				System.out.println("lookup <key>");
				System.out.println("delete <key>");
				break;
		}
		System.out.println("<key> is an integer value in the range 0 - 1000000");
		System.out.println();
	}
}
