package edu.uiuc.boltdb;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface represents the protocol between client and key-value store server.
 * @author Ashwin
 *
 */
public interface BoltDBProtocol extends Remote {

	/**
	 * The insert api is used to insert a key and a value into the store.
	 *'canBeForwarded' is a flag to indicate whether the request can be forwarded to
	 *other servers to perform the operation.
	 *Throws an exception if key is already present in the store.
	 * @param key
	 * @param value
	 * @param canBeForwarded
	 * @throws RemoteException
	 */
	public Boolean insert(long key, ValueTimeStamp value, boolean canBeForwarded, CONSISTENCY_LEVEL consistencyLevel) throws RemoteException;
	/**
	 * The lookup api is used to lookup the value associated with a key.
	 * Throws an exception is key is not present in the store.
	 * @param key
	 * @param canBeForwarded
	 * @return
	 * @throws RemoteException
	 */
	public ValueTimeStamp lookup(long key, boolean canBeForwarded, CONSISTENCY_LEVEL consistencyLevel) throws RemoteException;

	/**
	 * The update api updates the value of the provided key with the new value.
	 * Throws an exception if the key is not present.
	 * @param key
	 * @param value
	 * @param canBeForwarded
	 * @throws RemoteException
	 */
	public Boolean update(long key, ValueTimeStamp value, boolean canBeForwarded, CONSISTENCY_LEVEL consistencyLevel) throws RemoteException;

	/**
	 * The delete api removes the key-value entry from the store.
	 * Throws an exception is the key is not present in the store.
	 * @param key
	 * @param canBeForwarded
	 * @throws RemoteException
	 */
	public Boolean delete(long key, boolean canBeForwarded, CONSISTENCY_LEVEL consistencyLevel) throws RemoteException;
	
	public void lookupAndInsertInto(String hostname, long startKeyRange, long endKeyRange) throws RemoteException;
	
	public enum CONSISTENCY_LEVEL {
		ONE, QUORUM, ALL
	}
}
