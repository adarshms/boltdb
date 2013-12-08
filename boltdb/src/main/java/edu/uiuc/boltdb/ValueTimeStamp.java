package edu.uiuc.boltdb;

import java.io.Serializable;

public class ValueTimeStamp implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8293568407924175634L;
	public String value;
	public long timeStamp;
	
	public ValueTimeStamp(String value, long timeStamp) {
		super();
		this.value = value;
		this.timeStamp = timeStamp;
	}
	
}
