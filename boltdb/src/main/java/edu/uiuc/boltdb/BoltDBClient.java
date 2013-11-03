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

public class BoltDBClient {
	
	private BoltDBProtocol boltDBServer = null;
	
	public BoltDBClient(String rmiString) throws MalformedURLException, RemoteException, NotBoundException{
		boltDBServer = (BoltDBProtocol) Naming.lookup(rmiString + "/BoldDBServer");
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws NotBoundException 
	 */
	public static void main(String[] args) throws IOException, NotBoundException {
		// TODO Auto-generated method stub
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		String rmiString = prop.getProperty("boltdb.server");
		BoltDBClient boltDBClient = new BoltDBClient(rmiString);
		boltDBClient.runClientShell();
	}
	
	private void runClientShell() throws IOException{
		//TODO : Write code to similate a unix shell
		 String commandString = "";
		 BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
		
		 // Break out with Ctrl+C
		while (true) {
		  // Read user's input
		  System.out.print("boltdb>");
		  commandString = console.readLine();

		  // If the user entered a return, just loop again
		  if (commandString.equals(""))
			continue;
		  
		  
		}
	}
}
