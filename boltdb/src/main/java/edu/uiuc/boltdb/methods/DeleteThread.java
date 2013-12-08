package edu.uiuc.boltdb.methods;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.concurrent.Callable;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.BoltDBServer;
import edu.uiuc.boltdb.BoltDBProtocol.CONSISTENCY_LEVEL;
import edu.uiuc.boltdb.groupmembership.GroupMembership;
import edu.uiuc.boltdb.groupmembership.beans.Operation;

public class DeleteThread implements Callable<Boolean> {
	private String targetHost;
	private Long key;
	BoltDBProtocol.CONSISTENCY_LEVEL consistencyLevel;
	
	public DeleteThread(String targetHost, Long key,
			CONSISTENCY_LEVEL consistencyLevel) {
		super();
		this.targetHost = targetHost;
		this.key = key;
		this.consistencyLevel = consistencyLevel;
	}

	public Boolean call() {
		try {
		if (GroupMembership.membershipList.get(targetHost).hostname.equals(InetAddress.getLocalHost().getHostName())) {
			if(!BoltDBServer.KVStore.containsKey(key))
				return false;
				//throw new RemoteException("Key not present.");
			BoltDBServer.KVStore.remove(key);
			if(consistencyLevel != null)
				BoltDBServer.writeBuffer.add(new Operation("DELETE", consistencyLevel, new Date().toString(), key));
			return true;
		} else {
			BoltDBProtocol targetServer = (BoltDBProtocol) Naming
					.lookup("rmi://" + GroupMembership.membershipList.get(targetHost).hostname + "/KVStore");
			return targetServer.delete(key, false, consistencyLevel);
		}
		} catch(Exception e) {
			System.out.println("Exception in delete thread");
			return false;
		}
	}

}
