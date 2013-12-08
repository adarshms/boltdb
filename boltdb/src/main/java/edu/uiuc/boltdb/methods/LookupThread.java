package edu.uiuc.boltdb.methods;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.BoltDBServer;
import edu.uiuc.boltdb.BoltDBProtocol.CONSISTENCY_LEVEL;
import edu.uiuc.boltdb.ValueTimeStamp;
import edu.uiuc.boltdb.groupmembership.GroupMembership;

public class LookupThread implements Callable<ValueTimeStamp> {
	private String targetHost;
	private Long key;
	BoltDBProtocol.CONSISTENCY_LEVEL consistencyLevel;

	public LookupThread(String targetHost, Long key,
			CONSISTENCY_LEVEL consistencyLevel) {
		super();
		this.targetHost = targetHost;
		this.key = key;
		this.consistencyLevel = consistencyLevel;
	}

	public ValueTimeStamp call() {
		try {
			if (GroupMembership.membershipList.get(targetHost).hostname
					.equals(InetAddress.getLocalHost().getHostName())) {
				if(!BoltDBServer.KVStore.containsKey(key))
					return null;
					//throw new RemoteException("Key not present.");
				return BoltDBServer.KVStore.get(key);
			} else {
				BoltDBProtocol targetServer = (BoltDBProtocol) Naming
						.lookup("rmi://"
								+ GroupMembership.membershipList
										.get(targetHost).hostname + "/KVStore");
				return targetServer.lookup(key, false, consistencyLevel);
			}
		} catch (Exception e) {
			System.out.println("Exception in lookup thread");
			return null;
		}
	}
}
