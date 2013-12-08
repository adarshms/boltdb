package edu.uiuc.boltdb.examples;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.StringTokenizer;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.groupmembership.GroupMembership;

/**
 * 
 * @author Adarsh
 *
 */

public class MovieSearchClient {
	
	private BoltDBProtocol boltDBServer = null;
	
	public MovieSearchClient(String rmiString) throws MalformedURLException, RemoteException, NotBoundException {
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
			MovieSearchClient movieSearchClient = new MovieSearchClient(rmiString);
			movieSearchClient.runClient();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void runClient() throws IOException, NoSuchAlgorithmException {
		String keyWordString = "";
		BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
		 
	    System.out.println();
	    System.out.println("-----------------------------------------------");
	    System.out.println("MOVIE SEARCH");
	    System.out.println("-----------------------------------------------");

		while (true) {
		    System.out.print("Enter the keyword to search: ");
		    keyWordString = console.readLine();

		    // If the user entered a return, just loop again
		    if (keyWordString.equals("")) {
		    	System.out.println("You did not enter anything");
		    	System.out.println();
		    	continue;
		    }
		    searchMovie(keyWordString);
		}
	}
	
	private void searchMovie(String keyWordString) throws NoSuchAlgorithmException {
		try {
			StringTokenizer stk = new StringTokenizer(keyWordString);
			String keyWord = stk.nextToken();
			long key = computeHash(keyWord);

			String result = (String)boltDBServer.lookup(key, true);
			
			stk = new StringTokenizer(result, ",");
			
		    System.out.println();
		    System.out.println("-----------------------------------------------");
		    System.out.println("Found " + stk.countTokens() + " movie titles matchin " + " \"" + keyWord + "\"");
		    System.out.println("-----------------------------------------------");
		    while(stk.hasMoreTokens())
		    	System.out.println(stk.nextToken());
		    System.out.println("-----------------------------------------------");

		} catch(RemoteException re) {
			if(re.getCause().getCause() != null)
				System.out.println(re.getCause().getCause().getMessage());
			else
				System.out.println(re.getCause().getMessage());
		}
	}
	
	public static long computeHash(String pid) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		BigInteger bigInt = new BigInteger(1, md.digest(pid.getBytes()));
		return Math.abs(bigInt.longValue()) % 1000001L;
	}
}
