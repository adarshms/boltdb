package edu.uiuc.boltdb;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.TreeMap;

import edu.uiuc.boltdb.groupmembership.GroupMembership;

public class BoltDBServer extends UnicastRemoteObject implements BoltDBProtocol {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5195393553928167809L;

	protected BoltDBServer() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
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

	public void insert(long key, String value) throws RemoteException {
		KVStore.put(key, value);
	}

	public String lookup(long key) throws RemoteException {
		return KVStore.get(key);
	}

	public void update(long key, String value) throws RemoteException {
		KVStore.put(key, value);
	}

	public void delete(long key) throws RemoteException {
		KVStore.remove(key);
	}



}
