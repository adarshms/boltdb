package edu.uiuc.boltdb.groupmembership;


import org.apache.log4j.Logger;

/**
 * Thread to print out the bandwidth usage per minute.
 * @author ashwin
 *
 */
public class LogBandwidthThread implements Runnable {
	private static org.apache.log4j.Logger log = Logger.getRootLogger();

	public void run() {
		log.info("BANDWIDTH - - - "+GroupMembership.bandwidth+ " B/min");
		GroupMembership.bandwidth = 0;
	}
}
