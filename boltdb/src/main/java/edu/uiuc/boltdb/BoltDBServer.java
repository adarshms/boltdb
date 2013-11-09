package edu.uiuc.boltdb;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.GroupMembership;

public class BoltDBServer extends UnicastRemoteObject implements BoltDBProtocol {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5195393553928167809L;
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	
	protected BoltDBServer() throws RemoteException {
		super();
	}

	/**
	 * @param args
	 */
	public static Map<Long,String> KVStore = new ConcurrentHashMap<Long,String>();


	public static void main(String[] args) throws IOException {
		
		Runnable groupMembership = new GroupMembership(args);
		Thread groupMembershipThread = new Thread(groupMembership);
		groupMembershipThread.start();
		LocateRegistry.createRegistry(1099);
		Naming.rebind ("KVStore", new BoltDBServer());
	}
	
	public void insert(long key, String value, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(KVStore.containsKey(key))
				throw new RemoteException("Key already present.");
			KVStore.put(key, value);
			return;
		}
		String targetHost = null;
		try {
			targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				if(KVStore.containsKey(key))
					throw new RemoteException("Key already present.");
				KVStore.put(key, value);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.insert(key, value, false);
			}
		} catch(RemoteException re) {
			throw re;
		} catch (Exception e) {
			log.error("ERROR" , e);
			throw new RemoteException("Error occured at Server");
		} 
	}

	public String lookup(long key, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			return KVStore.get(key);
		}
		String targetHost = null;
		try {
			targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				if(!KVStore.containsKey(key))
					throw new RemoteException("Key not present.");
				return KVStore.get(key);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				return targetServer.lookup(key, false);
			}
		} catch(RemoteException re) {
			throw re;
		} catch (Exception e) {
			log.error("ERROR" , e);
			throw new RemoteException("Error occured at Server");
		}
	}

	public void update(long key, String value, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			KVStore.put(key, value);
			return;
		}
		String targetHost = null;
		try {
			targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				if(!KVStore.containsKey(key))
					throw new RemoteException("Key not present.");
				KVStore.put(key, value);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.update(key, value, false);
			}
		} catch(RemoteException re) {
			throw re;
		} catch (Exception e) {
			log.error("ERROR" , e);
			throw new RemoteException("Error occured at Server");
		}
	}

	public void delete(long key, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			KVStore.remove(key);
			return;
		}
		String targetHost = null;
		try {
			targetHost = GroupMembership.getSuccessorNodeOf(GroupMembership.computeHash((new Long(key).toString())));
			if(targetHost.equals(InetAddress.getLocalHost().getHostName())) {
				if(!KVStore.containsKey(key))
					throw new RemoteException("Key not present.");
				KVStore.remove(key);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + targetHost + "/KVStore");
				targetServer.delete(key, false);
			}
		} catch(RemoteException re) {
			throw re;
		} catch (Exception e) {
			log.error("ERROR" , e);
			throw new RemoteException("Error occured at Server");
		}		
	}
}
