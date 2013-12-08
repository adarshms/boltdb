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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import edu.uiuc.boltdb.BoltDBProtocol.CONSISTENCY_LEVEL;
import edu.uiuc.boltdb.groupmembership.GroupMembership;
import edu.uiuc.boltdb.methods.DeleteThread;
import edu.uiuc.boltdb.methods.InsertThread;
import edu.uiuc.boltdb.methods.LookupThread;
import edu.uiuc.boltdb.methods.UpdateThread;

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
	public static ConcurrentMap<Long,ValueTimeStamp> KVStore = new ConcurrentHashMap<Long,ValueTimeStamp>();


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
	
	private int convertConsistencyLevelToInt(CONSISTENCY_LEVEL consistencyLevel) {
		switch(consistencyLevel) {
		case ALL:
			return GroupMembership.replicationFactor;
		case ONE:
			return 1;
		case QUORUM:
			return GroupMembership.replicationFactor/2 + 1;
		default:
			break;
		 
		}
		return -1;
	}
	
	private boolean waitForReplicaReplies(ExecutorCompletionService<Boolean> completionService, CONSISTENCY_LEVEL consistencyLevel) throws InterruptedException, ExecutionException {
		int replicaRepliesToWaitFor = convertConsistencyLevelToInt(consistencyLevel);
		
		for(int i = 0; i < GroupMembership.replicationFactor; i++) {
			final Future<Boolean> future = completionService.take();
			if(future.get() == true) {
				replicaRepliesToWaitFor--;
				if (replicaRepliesToWaitFor == 0) {
					return true;
				}
			}
		}
		return false;
	}
	
	private ValueTimeStamp waitForReplicaRepliesForUpdate(ExecutorCompletionService<ValueTimeStamp> completionService, CONSISTENCY_LEVEL consistencyLevel) throws InterruptedException, ExecutionException {
		int replicaRepliesToWaitFor = convertConsistencyLevelToInt(consistencyLevel);
		ValueTimeStamp result = null;
		ValueTimeStamp temp;
		for(int i = 0; i < GroupMembership.replicationFactor; i++) {
			final Future<ValueTimeStamp> future = completionService.take();
			if((temp = future.get()) != null) {
				if (result == null) result = temp;
				else {
					if(result.timeStamp < temp.timeStamp)
						result = temp;
				}
				replicaRepliesToWaitFor--;
				if (replicaRepliesToWaitFor == 0) {
					return result;
				}
			}
		}
		return null;
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

	public Boolean insert(long key, ValueTimeStamp value, boolean canBeForwarded,CONSISTENCY_LEVEL consistencyLevel) throws RemoteException {
		if(!canBeForwarded) {
			if(KVStore.containsKey(key))
				throw new RemoteException("Key already present.");
			KVStore.put(key, value);
			return true;
		}
		
		value.timeStamp = System.currentTimeMillis();
		try {
			// Determine the target host using the getSuccessorNodeOf method
			
			String[] targetHosts = getTargetHosts(key);
			
			final ExecutorService pool = Executors.newFixedThreadPool(GroupMembership.replicationFactor);
			final ExecutorCompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(pool);
			
			for(int i = 0; i < GroupMembership.replicationFactor; i++) {
				completionService.submit(new InsertThread(targetHosts[i],key,value,consistencyLevel));
			}
			boolean result = waitForReplicaReplies(completionService, consistencyLevel);
			pool.shutdown();
			return result;

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
	public ValueTimeStamp lookup(long key, boolean canBeForwarded, CONSISTENCY_LEVEL consistencyLevel) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			return KVStore.get(key);
		}

		try {
			String[] targetHosts = getTargetHosts(key);
			
			final ExecutorService pool = Executors.newFixedThreadPool(GroupMembership.replicationFactor);
			final ExecutorCompletionService<ValueTimeStamp> completionService = new ExecutorCompletionService<ValueTimeStamp>(pool);
			
			for(int i = 0; i < GroupMembership.replicationFactor; i++) {
				completionService.submit(new LookupThread(targetHosts[i],key,consistencyLevel));
			}
			
			ValueTimeStamp result = waitForReplicaRepliesForUpdate(completionService, consistencyLevel);
			pool.shutdown();
			return result;
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
	public Boolean update(long key, ValueTimeStamp value, boolean canBeForwarded, CONSISTENCY_LEVEL consistencyLevel) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			if(KVStore.get(key).timeStamp < value.timeStamp)
				KVStore.put(key, value);
			return true;
		}
		value.timeStamp = System.currentTimeMillis();
		try {
			String[] targetHosts = getTargetHosts(key);
			
			final ExecutorService pool = Executors.newFixedThreadPool(GroupMembership.replicationFactor);
			final ExecutorCompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(pool);
			
			for(int i = 0; i < GroupMembership.replicationFactor; i++) {
				completionService.submit(new UpdateThread(targetHosts[i],key,value,consistencyLevel));
			}
			
			boolean result = waitForReplicaReplies(completionService, consistencyLevel);
			pool.shutdown();
			return result;
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
	public Boolean delete(long key, boolean canBeForwarded,CONSISTENCY_LEVEL consistencyLevel) throws RemoteException {
		if(!canBeForwarded) {
			if(!KVStore.containsKey(key))
				throw new RemoteException("Key not present.");
			KVStore.remove(key);
			return true;
		}

		try {
			String[] targetHosts = getTargetHosts(key);
			
			final ExecutorService pool = Executors.newFixedThreadPool(GroupMembership.replicationFactor);
			final ExecutorCompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(pool);
			
			for(int i = 0; i < GroupMembership.replicationFactor; i++) {
				completionService.submit(new DeleteThread(targetHosts[i],key,consistencyLevel));
			}
			
			boolean result = waitForReplicaReplies(completionService, consistencyLevel);
			pool.shutdown();
			return result;
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
		for(Entry<Long,ValueTimeStamp> e : KVStore.entrySet()) {
			try {
				long hashOfKey = GroupMembership.computeHash(e.getKey().toString());
				System.out.println("lookupAndInsert: hashOfkey - "+hashOfKey);
				if (startKeyRange < endKeyRange) {
					if (hashOfKey >= startKeyRange && hashOfKey <= endKeyRange) {
						System.out.println("Inserting " + hashOfKey + " from "
								+ GroupMembership.pid + " to " + hostname);
						targetServer.insert(e.getKey(), e.getValue(), false,null);
					}
				} else {
					if (hashOfKey >= startKeyRange || hashOfKey <= endKeyRange) {
						System.out.println("Inserting " + hashOfKey + " from "
								+ GroupMembership.pid + " to " + hostname);
						targetServer.insert(e.getKey(), e.getValue(), false,null);
					}
				}
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
}
