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
import java.security.NoSuchAlgorithmException;
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
	public static Map<Long,String> KVStore = new TreeMap<Long,String>();
	
	public static void main(String[] args) throws IOException {
		
		Runnable groupMembership = new GroupMembership(args);
		Thread groupMembershipThread = new Thread(groupMembership);
		groupMembershipThread.start();
		LocateRegistry.createRegistry(1099);
		Naming.rebind ("KVStore", new BoltDBServer());
	}
	
	public void insert(long key, String value, boolean canBeForwarded) throws RemoteException {
		String targetHost = null;
		try {
			targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				KVStore.put(key, value);
			} else if(canBeForwarded){
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.insert(key, value, false);
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

	public String lookup(long key, boolean canBeForwarded) throws RemoteException {
		if(checkLocalStore(key)) {
			return KVStore.get(key);
		} else if(canBeForwarded){
			try {
				String targetHost = null;
				try {
					targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				return targetServer.lookup(key, false);
			} catch (MalformedURLException e) {
					e.printStackTrace();
			} catch (NotBoundException e) {
					e.printStackTrace();
			}
		}
		return null;

	}

	public void update(long key, String value, boolean canBeForwarded) throws RemoteException {
		String targetHost = null;
		try {
			targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				KVStore.put(key, value);
			} else if(canBeForwarded) {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.update(key, value, false);
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

	public void delete(long key, boolean canBeForwarded) throws RemoteException {
		if(checkLocalStore(key)) {
			KVStore.remove(key);
			return;
		} else if(canBeForwarded){
			try {
				String targetHost = null;
				try {
					targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.delete(key, false);
				return;
			} catch (MalformedURLException e) {
					e.printStackTrace();
			} catch (NotBoundException e) {
					e.printStackTrace();
			}
		}


	}

	private boolean checkLocalStore(long key) {
		return KVStore.containsKey(key);
	}
}
