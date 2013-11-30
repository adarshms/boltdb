package edu.uiuc.boltdb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.groupmembership.GroupMembership;

/**
 * This class represents the server component of the distributed key value store. It implements the 
 * BoltDBProtocol interface. On startup, it starts the GroupMembership service. It maintains a
 * ConcurrentHashMap KVStore that stores the key-value pairs that are destined for this host. Since it implements
 * the BoltDBProtocol, it provides the Remote methods to insert, lookup, update and delete keys. Upon request
 * from a client, depending on the target host and the canBeForwarded flag, it performs the operation on 
 * the local key value store (If it is the target host) or forwards the operation by determining the target
 * host from its MembershipList. 
 * @author Adarsh
 *
 */

public class BoltDBServer extends UnicastRemoteObject implements BoltDBProtocol {

	private static final long serialVersionUID = 5195393553928167809L;
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	
	protected BoltDBServer() throws RemoteException {
		super();
	}

	/**
	 * @param args
	 */
	public static SortedMap<Long,String> KVStore = Collections.synchronizedSortedMap(new TreeMap<Long,String>());


	public static void main(String[] args) throws IOException {
		
		// Start the GroupMembership service
		Runnable groupMembership = new GroupMembership(args);
		Thread groupMembershipThread = new Thread(groupMembership);
		groupMembershipThread.start();
		
		//Create RMI registry
		LocateRegistry.createRegistry(1099);
		Naming.rebind ("KVStore", new BoltDBServer());
	}
	
	/**
	 * The insert api is used to insert a key and a value into the store.
	 *'canBeForwarded' is a flag to indicate whether the request can be forwarded to
	 *other servers to perform the operation.
	 *Throws an exception if key is already present in the store.
	 * @param key
	 * @param value
	 * @param canBeForwarded
	 * @throws RemoteException
	 */

	public void insert(long key, String value, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(KVStore.containsKey(key))
				throw new RemoteException("Key already present.");
			KVStore.put(key, value);
			return;
		}
		String targetHost = null;
		try {
			// Determine the target host using the getSuccessorNodeOf method
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

	/**
	 * The lookup api is used to lookup the value associated with a key.
	 * Throws an exception is key is not present in the store.
	 * @param key
	 * @param canBeForwarded
	 * @return
	 * @throws RemoteException
	 */
	public String lookup(long key, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			return KVStore.get(key);
		}
		String targetHost = null;
		try {
			// Determine the target host using the getSuccessorNodeOf method
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

	/**
	 * The update api updates the value of the provided key with the new value.
	 * Throws an exception if the key is not present.
	 * @param key
	 * @param value
	 * @param canBeForwarded
	 * @throws RemoteException
	 */
	public void update(long key, String value, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			KVStore.put(key, value);
			return;
		}
		String targetHost = null;
		try {
			// Determine the target host using the getSuccessorNodeOf method
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

	/**
	 * The delete api removes the key-value entry from the store.
	 * Throws an exception is the key is not present in the store.
	 * @param key
	 * @param canBeForwarded
	 * @throws RemoteException
	 */
	public void delete(long key, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			KVStore.remove(key);
			return;
		}
		String targetHost = null;
		try {
			// Determine the target host using the getSuccessorNodeOf method
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

	public void lookupAndInsertInto(String hostname, long startKeyRange,
			long endKeyRange) throws  RemoteException {
		BoltDBProtocol targetServer = null;
		try {
			targetServer = (BoltDBProtocol) Naming.lookup("rmi://" + hostname + "/KVStore");
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		SortedMap<Long,String> submap = KVStore.subMap(startKeyRange, endKeyRange);
		
		for(Entry<Long,String> e : submap.entrySet()) {
			targetServer.insert(e.getKey(), e.getValue(), false);
		}
	}
}
