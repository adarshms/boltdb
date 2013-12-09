package edu.uiuc.boltdb.groupmembership.beans;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.BoltDBProtocol.CONSISTENCY_LEVEL;

public class Operation {

	String operation;
	BoltDBProtocol.CONSISTENCY_LEVEL consistency;
	String time;
	long key;
	String value;
	
	public Operation(String operation, BoltDBProtocol.CONSISTENCY_LEVEL consistency, String time,
			long key, String value) {
		super();
		this.operation = operation;
		this.consistency = consistency;
		this.time = time;
		this.key = key;
		this.value = value;
	}

	
	public Operation(String operation, CONSISTENCY_LEVEL consistency,
			String time, long key) {
		super();
		this.operation = operation;
		this.consistency = consistency;
		this.time = time;
		this.key = key;
	}


	@Override
	public String toString() {
		if (operation.equals("DELETE")) {
			return "[OPERATION: "+operation+" CONSISTENCY: "+consistency+" TIME: "+time+" KEY: "+key+"]";
		}
		return "[OPERATION: "+operation+" CONSISTENCY: "+consistency+" TIME: "+time+" KEY: "+key+" VALUE: "+value+"]";
	}

}
