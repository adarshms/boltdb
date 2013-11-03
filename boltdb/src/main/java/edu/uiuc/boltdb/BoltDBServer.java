package edu.uiuc.boltdb;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.TreeMap;

import edu.uiuc.boltdb.groupmembership.GroupMembership;

public class BoltDBServer extends UnicastRemoteObject implements BoltDBProtocol<Long,Object> {

	
	private static final long serialVersionUID = 1L;

	protected BoltDBServer() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	private static Map<Long,Object> KVStore = new TreeMap<Long,Object>();
	
	public static void main(String[] args) {
		
		Runnable groupMembership = new GroupMembership(args);
		Thread groupMembershipThread = new Thread(groupMembership);
		groupMembershipThread.start();
	}

	public void insert(Long key, Object value) throws RemoteException {
		KVStore.put(key, value);
	}

	public Object lookup(Long key) throws RemoteException {
		return KVStore.get(key);
	}

	public void update(Long key, Object value) throws RemoteException {
		KVStore.put(key, value);
	}

	public void delete(Long key) throws RemoteException {
		KVStore.remove(key);
	}



}
