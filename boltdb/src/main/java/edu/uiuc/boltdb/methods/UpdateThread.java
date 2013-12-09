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
import edu.uiuc.boltdb.ValueTimeStamp;

public class UpdateThread implements Callable<Boolean> {

	private String targetHost;
	private Long key;
	private ValueTimeStamp value;
	BoltDBProtocol.CONSISTENCY_LEVEL consistencyLevel;
	
	public UpdateThread(String targetHost, Long key, ValueTimeStamp value,
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
			if(!BoltDBServer.KVStore.containsKey(key))
				return false;
				//throw new RemoteException("Key not present.");
				
			if(BoltDBServer.KVStore.get(key).timeStamp < value.timeStamp) 
				BoltDBServer.KVStore.put(key, value);
			if(consistencyLevel != null)
				BoltDBServer.writeBuffer.add(new Operation("UPDATE", consistencyLevel, new Date().toString(), key, value.value));
			return true;
		} else {
			BoltDBProtocol targetServer = (BoltDBProtocol) Naming
					.lookup("rmi://" + GroupMembership.membershipList.get(targetHost).hostname + "/KVStore");
			return targetServer.update(key, value, false, consistencyLevel);
		}
		} catch(Exception e) {
			System.out.println("Exception in update thread");
			return false;
		}
	}

}
