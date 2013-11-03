package edu.uiuc.boltdb;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface BoltDBProtocol<K,V> extends Remote {

	public void insert(K key, V value) throws RemoteException;
	
	public V lookup(K key) throws RemoteException;

	public void update(K key, V value) throws RemoteException;

	public void delete(K key) throws RemoteException;

}
