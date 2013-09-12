package edu.uiuc.boltdb.logquerier.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.tools.ant.taskdefs.optional.clearcase.ClearCase;

import edu.uiuc.boltdb.logquerier.utils.ClientArgs;

public class LogQuerierClient {

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		runclient(args);
	}

	private static void runclient(String[] args) throws FileNotFoundException,
			IOException {
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		ClientArgs clientArgs = new ClientArgs();
		// TODO error cases needed;check for spaces,incorrect option
		StringBuilder options = new StringBuilder();
		for (int j = 0; j < args.length; j++) {
			if (args[j].equals("-key")) {
				clientArgs.setKeyRegExp(args[++j]);
			} else if (args[j].equals("-value")) {
				clientArgs.setValRegExp(args[++j]);
			} else  {
				options.append(args[j]);
				options.append(" ");
			}
		}
		clientArgs.addOption(options.toString());
		if (clientArgs.getKeyRegExp().isEmpty()
				&& clientArgs.getValRegExp().isEmpty()) {
			System.out.println("Error : Both key and value missing");
		}

		String[] addresses = prop.getProperty("machines.address").split(",");
		LinkedList<Thread> clientThreads = new LinkedList<Thread>();
		for (int i = 0; i < addresses.length; i++) {
			String[] hostPort = addresses[i].split(":");
			// TODO error conditions
			InetAddress address = InetAddress.getByName(hostPort[0]);
			Thread newThread = new LogQuerierClientThread(address,
					Integer.parseInt(hostPort[1]), clientArgs);
			newThread.start();
			clientThreads.add(newThread);
		}
		int completedThreads = 0;
		while(completedThreads != addresses.length) {
			for(int i = 0; i < clientThreads.size(); i++) {
				if(!clientThreads.get(i).isAlive()) {
					completedThreads++;
					printOutput(clientThreads.get(i));
					clientThreads.remove(i);
				}
			}
		}

	}
	
	private static void printOutput(Thread t) throws IOException {
		LogQuerierClientThread lct = (LogQuerierClientThread) t;
		File outputFile = new File("output-"+lct.getAddress().getHostAddress()+"."+lct.getPort());
		if(outputFile.exists()) {
			System.out.println();
			BufferedReader br = new BufferedReader(new FileReader(outputFile));
			String line;
			System.out.println("Logs from "+ lct.getAddress()+":"+lct.getPort());
			while((line = br.readLine()) != null) {
				System.out.println(line);
			}
			br.close();
			outputFile.delete();
		} /*else {
			System.out.println(outputFile+" doesnt exists");
		}*/
	}
}
