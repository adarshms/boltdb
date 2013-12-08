package edu.uiuc.boltdb.logquerier.tests;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import edu.uiuc.boltdb.BoltDBProtocol;

public class BoltDBPerformanceTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 * @throws NotBoundException 
	 */
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, NotBoundException {
		// TODO Auto-generated method stub
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		String rmiString = prop.getProperty("boltdb.kvstore.server");
		String rawFile = prop.getProperty("boltdb.kvstore.examples.movies.toIndex");
		
		if(System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		BoltDBProtocol boltDBServer = (BoltDBProtocol) Naming.lookup("rmi://" + rmiString + "/KVStore");
		
		HashMap<Long, String> hmap = new HashMap<Long, String>();
		BufferedReader br = new BufferedReader(new FileReader(rawFile));
		String line;
		int count = 0;
		while((line = br.readLine()) != null && count < 1000) {
			line = line.trim();
			StringTokenizer stk = new StringTokenizer(line);
			while(stk.hasMoreTokens() && count < 1000) {
				long key = computeHash(stk.nextToken());
				String curValue = hmap.get(key);
				if(curValue != null)
					continue;
				String value = line; 
				hmap.put(key, value);
				count++;
			}
		}
		Iterator<Entry<Long,String>> itr = hmap.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<Long,String> entry = itr.next();
			boltDBServer.insert(entry.getKey(), entry.getValue(), true);
			//System.out.println("Successfully inserted " + entry.getKey() + " --> " + entry.getValue());
		}
		
		int updateCount = 0;
		itr = hmap.entrySet().iterator();
		while(itr.hasNext() && updateCount++ < 100) {
			Entry<Long,String> entry = itr.next();
			long startTime = System.currentTimeMillis();
			boltDBServer.update(entry.getKey(), "Some value", true);
			long endTime = System.currentTimeMillis();
			System.out.println(endTime-startTime);
		}
		
		System.out.println("------------------------------------------------------------");
		
		int deleteCount = 0;
		itr = hmap.entrySet().iterator();
		while(itr.hasNext() && deleteCount++ < 100) {
			Entry<Long,String> entry = itr.next();
			long startTime = System.currentTimeMillis();
			boltDBServer.delete(entry.getKey(), true);
			long endTime = System.currentTimeMillis();
			System.out.println(endTime-startTime);
		}
		
		br.close();
	}
	public static long computeHash(String pid) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
		BigInteger bigInt = new BigInteger(1, md.digest(pid.getBytes()));
		return Math.abs(bigInt.longValue()) % 1000001L;
		}
	}
