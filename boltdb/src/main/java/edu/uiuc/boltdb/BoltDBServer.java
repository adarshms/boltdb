package edu.uiuc.boltdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.TreeMap;

import edu.uiuc.boltdb.groupmembership.GroupMembership;
import edu.uiuc.boltdb.groupmembership.beans.MembershipBean;

public class BoltDBServer extends UnicastRemoteObject implements BoltDBProtocol {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5195393553928167809L;

	protected BoltDBServer() throws RemoteException {
		super();
	}

	/**
	 * @param args
	 */
	private static Map<Long,String> KVStore = new TreeMap<Long,String>();
	
	public static void main(String[] args) throws RemoteException, MalformedURLException {
		
		Runnable groupMembership = new GroupMembership(args);
		Thread groupMembershipThread = new Thread(groupMembership);
		groupMembershipThread.start();
		LocateRegistry.createRegistry(1099);
		Naming.rebind ("KVStore", new BoltDBServer());
        System.out.println ("Server is ready.");
        
	}
	
	private void runServerShell() throws IOException {
		// Simulate a unix shell
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
		  else if (commandString.equals("show")) {
			  System.out.println("-------------------------------------------------");
			  System.out.println("Membership List : ");
			  System.out.println("-------------------------------------------------");
			  for (Map.Entry<String, MembershipBean> entry : GroupMembership.membershipList.entrySet())
			  {
				    System.out.println(entry);
			  }
			  System.out.println("-------------------------------------------------");
			  System.out.println();
			  System.out.println("-------------------------------------------------");
			  System.out.println("Key Value Store : ");
			  System.out.println("-------------------------------------------------");
			  for (Map.Entry<Long, String> entry : KVStore.entrySet())
			  {
				    System.out.println(entry.getKey() + " ---> " + entry.getValue());
			  }
			  System.out.println("-------------------------------------------------");
			  System.out.println();
		  }
		}
	}

	public void insert(long key, String value) throws RemoteException {
		String targetHost = getTargetHost(key);
		try {
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				KVStore.put(key, value);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.insert(key, value);
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String lookup(long key) throws RemoteException {
		if(checkLocalStore(key)) {
			return KVStore.get(key);
		} else {
			try {
				String targetHost = getTargetHost(key);
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				return targetServer.lookup(key);
			} catch (MalformedURLException e) {
					e.printStackTrace();
			} catch (NotBoundException e) {
					e.printStackTrace();
			}
		}
		return null;

	}

	public void update(long key, String value) throws RemoteException {
		String targetHost = getTargetHost(key);
		try {
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				KVStore.put(key, value);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.update(key, value);
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void delete(long key) throws RemoteException {
		if(checkLocalStore(key)) {
			KVStore.remove(key);
			return;
		} else {
			try {
				String targetHost = getTargetHost(key);
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.delete(key);
				return;
			} catch (MalformedURLException e) {
					e.printStackTrace();
			} catch (NotBoundException e) {
					e.printStackTrace();
			}
		}


	}

	private boolean checkLocalStore(long key) {
		return KVStore.containsKey(computeHash((new Long(key)).toString()));
	}
	
	private String getTargetHost(long key) {
		long keyHash = computeHash((new Long(key).toString()));
		String targetHost = null;
		String firstHost = null;
		
		for (Map.Entry<String, MembershipBean> entry : GroupMembership.membershipList.entrySet())
		{
		    if(keyHash <= entry.getValue().hashValue) {
		    	targetHost = entry.getValue().hostname;
		    	continue;
		    }
		    else 
		    	return targetHost;
		}
		return firstHost;
	}
	
	private long computeHash(String pid) {
		long hashValue = 13;
		for (int i=0; i < pid.length(); i++) {
		    hashValue = hashValue*31 + pid.charAt(i);
		}
		return hashValue % 1000001L;
	}
}
