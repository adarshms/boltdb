package edu.uiuc.boltdb;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;


public class BoltDBClientTemp {

	/**
	 * @param args
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws MalformedURLException 
	 */
	public static void main(String[] args) throws MalformedURLException, RemoteException, NotBoundException {
		// TODO Auto-generated method stub
		System.out.println("starting security manager");
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		BoltDBProtocol boltdb = 
		        (BoltDBProtocol) Naming.lookup ("//localhost/KVStore");
		boltdb.insert(123l, "a");
		System.out.println(boltdb.lookup(123l));
		
	}

}
