package edu.uiuc.boltdb.examples;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import edu.uiuc.boltdb.BoltDBProtocol;
import edu.uiuc.boltdb.ValueTimeStamp;
import edu.uiuc.boltdb.groupmembership.HeartbeatIncrementerThread;

public class StaleClient {

	private static volatile long i = 0;
	static int stalelookups = 0;
	static long noOfwrites = 0;
	static long noOfreads = 0;
	/**
	 * @param args
	 * @throws IOException 
	 * @throws NotBoundException 
	 */
	public static void main(String[] args) throws IOException, NotBoundException {
		// TODO Auto-generated method stub
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		String rmiString = prop.getProperty("boltdb.kvstore.server");
		if(System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		final BoltDBProtocol boltDBServer = (BoltDBProtocol) Naming.lookup("rmi://" + rmiString + "/KVStore");
		boltDBServer.insert(10, new ValueTimeStamp(String.valueOf(i), 0), true, BoltDBProtocol.CONSISTENCY_LEVEL.ONE);
		final int readwait = Integer.parseInt(prop.getProperty("staleclient.readwait"));
		final int writewait = Integer.parseInt(prop.getProperty("staleclient.writewait"));

		Runnable writeClient = new Runnable() {

			public void run() {
				while(true) {
					try {
						boltDBServer.update(10, new ValueTimeStamp(String.valueOf(++i), 0), true, BoltDBProtocol.CONSISTENCY_LEVEL.ONE);
						noOfwrites++;
						Thread.sleep(writewait);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		
		Runnable readClient = new Runnable() {

			public void run() {
				while(true) {
					try {
						int result = Integer.parseInt(boltDBServer.lookup(10, true, BoltDBProtocol.CONSISTENCY_LEVEL.ONE).value);
						noOfreads++;
						if(result != i) {
							stalelookups++;
						}
						Thread.sleep(readwait);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		
		Runnable printStaleValue = new Runnable() {
			
			public void run() {
				// TODO Auto-generated method stub
				System.out.println("Stale values per sec:"+stalelookups +" Writes:"+noOfwrites+" Reads:"+noOfreads);
				stalelookups = 0;
				noOfreads = 0;
				noOfwrites = 0;
			}
		};
		
		ScheduledExecutorService scheduler = Executors
				.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(printStaleValue, 0,
				Integer.parseInt(prop
						.getProperty("staleclient.printfreq")),
				TimeUnit.MILLISECONDS);
		
		Thread writeThread = new Thread(writeClient);
		Thread readThread = new Thread(readClient);
		writeThread.start();
		readThread.start();
		
		
	}

}
