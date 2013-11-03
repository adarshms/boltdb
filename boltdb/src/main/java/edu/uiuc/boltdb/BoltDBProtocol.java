package edu.uiuc.boltdb;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface BoltDBProtocol extends Remote {

	public void insert(long key, String value) throws RemoteException;
	
	public String lookup(long key) throws RemoteException;

	public void update(long key, String value) throws RemoteException;

	public void delete(long key) throws RemoteException;

}
