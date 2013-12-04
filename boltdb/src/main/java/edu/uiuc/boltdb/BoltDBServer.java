package edu.uiuc.boltdb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.security.NoSuchAlgorithmException;
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
	
	private String[] getTargetHosts(long key) throws NoSuchAlgorithmException {
		String targetHost = null;
		String[] targetHosts = new String[GroupMembership.replicationFactor];
		targetHost = GroupMembership.getSuccessorNode(GroupMembership.computeHash((new Long(key).toString())));
		if(targetHost != null) {
			targetHosts[0] = targetHost;
		} else {
			log.error("Target host is null");
			return null;
		}
		
		for(int i = 1; i < GroupMembership.replicationFactor; i++) {
			targetHosts[i] = GroupMembership.getSuccessorNode(GroupMembership.membershipList.get(targetHosts[i-1]).hashValue);
		}
		return targetHosts;
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
	 * @throws  
	 */

	public void insert(long key, String value, boolean canBeForwarded) throws RemoteException {
		if(!canBeForwarded) {
			if(KVStore.containsKey(key))
				throw new RemoteException("Key already present.");
			KVStore.put(key, value);
			return;
		}
		
		try {
			// Determine the target host using the getSuccessorNodeOf method
			
			String[] targetHosts = getTargetHosts(key);

			for (int i = 0; i < GroupMembership.replicationFactor; i++) {
				if (GroupMembership.membershipList.get(targetHosts[i]).hostname.equals(InetAddress.getLocalHost().getHostName())) {
					if (KVStore.containsKey(key))
						throw new RemoteException("Key already present.");
					KVStore.put(key, value);
				} else {
					BoltDBProtocol targetServer = (BoltDBProtocol) Naming
							.lookup("rmi://" + GroupMembership.membershipList.get(targetHosts[i]).hostname + "/KVStore");
					targetServer.insert(key, value, false);
				}
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
			targetHost = GroupMembership.membershipList.get(GroupMembership.getSuccessorNode(GroupMembership.computeHash((new Long(key).toString())))).hostname;
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
			targetHost = GroupMembership.membershipList.get(GroupMembership.getSuccessorNode(GroupMembership.computeHash((new Long(key).toString())))).hostname;
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
			targetHost = GroupMembership.membershipList.get(GroupMembership.getSuccessorNode(GroupMembership.computeHash((new Long(key).toString())))).hostname;
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
		System.out.println("lookupAndInsert: start - "+startKeyRange+" end - "+ endKeyRange);
		for(Entry<Long,String> e : KVStore.entrySet()) {
			try {
				long hashOfKey = GroupMembership.computeHash(e.getKey().toString());
				System.out.println("lookupAndInsert: hashOfkey - "+hashOfKey);
				if (startKeyRange < endKeyRange) {
					if (hashOfKey >= startKeyRange && hashOfKey <= endKeyRange) {
						System.out.println("Inserting " + hashOfKey + " from "
								+ GroupMembership.pid + " to " + hostname);
						targetServer.insert(e.getKey(), e.getValue(), false);
					}
				} else {
					if (hashOfKey >= startKeyRange || hashOfKey <= endKeyRange) {
						System.out.println("Inserting " + hashOfKey + " from "
								+ GroupMembership.pid + " to " + hostname);
						targetServer.insert(e.getKey(), e.getValue(), false);
					}
				}
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
}
