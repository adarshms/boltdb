package edu.uiuc.boltdb.methods;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.BoltDBProtocol.CONSISTENCY_LEVEL;
import edu.uiuc.boltdb.BoltDBServer;
import edu.uiuc.boltdb.ValueTimeStamp;
import edu.uiuc.boltdb.groupmembership.GroupMembership;

public class InsertThread implements Callable<Boolean> {
	private String targetHost;
	private Long key;
	private ValueTimeStamp value;
	BoltDBProtocol.CONSISTENCY_LEVEL consistencyLevel;
	
	public InsertThread(String targetHost, Long key, ValueTimeStamp value,
			CONSISTENCY_LEVEL consistencyLevel) {
		super();
		this.targetHost = targetHost;
		this.key = key;
		this.value = value;
		this.consistencyLevel = consistencyLevel;
	}


	public Boolean call() {
		try {
		if (GroupMembership.membershipList.get(targetHost).hostname.equals(InetAddress.getLocalHost().getHostName())) {
			if (BoltDBServer.KVStore.containsKey(key))
				return false;
				//throw new RemoteException("Key already present.");
			BoltDBServer.KVStore.put(key, value);
			return true;
		} else {
			BoltDBProtocol targetServer = (BoltDBProtocol) Naming
					.lookup("rmi://" + GroupMembership.membershipList.get(targetHost).hostname + "/KVStore");
			return targetServer.insert(key, value, false, consistencyLevel);
		}
		} catch(Exception e) {
			System.out.println("Exception in insert thread");
			return false;
		}
	}

}
