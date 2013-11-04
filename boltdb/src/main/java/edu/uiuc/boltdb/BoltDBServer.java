package edu.uiuc.boltdb;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

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
	public static Map<Long,String> KVStore = new HashMap<Long,String>();
	
	public static void main(String[] args) throws RemoteException, MalformedURLException {
		
		Runnable groupMembership = new GroupMembership(args);
		Thread groupMembershipThread = new Thread(groupMembership);
		groupMembershipThread.start();
		LocateRegistry.createRegistry(1099);
		Naming.rebind ("KVStore", new BoltDBServer());
        System.out.println ("Server is ready.");
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
