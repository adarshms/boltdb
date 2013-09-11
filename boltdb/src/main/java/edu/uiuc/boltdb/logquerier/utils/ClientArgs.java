package edu.uiuc.boltdb.logquerier.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class ClientArgs implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1297360167585337435L;
	private String keyRegExp = new String();
	private String valRegExp = new String();
	private String optionsStr = new String();
	

	public String getKeyRegExp() {
		return keyRegExp;
	}

	public String getValRegExp() {
		return valRegExp;
	}

	public void setKeyRegExp(String regExp) {
		keyRegExp = regExp;
	}

	public void setValRegExp(String regExp) {
		valRegExp = regExp;
	}
	
	public void addOption(String option)
	{
		optionsStr += " " + option;
	}
	
	public String getOptionsString()
	{
		return optionsStr;
	}
}