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
/**
 * This class represents the distributed log querier client which is run by the dgrep command.
 * Firstly,the command line options are parsed.
 * Then, the list of machines to connect to are picked up from a property file called
 * boltdb.prop and spawns a thread for each ,which talks to the servers and gets and
 * the grepped logs. 
 * 
 * @author ashwin
 *
 */
public class LogQuerierClient {

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		runclient(args);
	}

	private static void runclient(String[] args) throws FileNotFoundException,
			IOException {
		//Get the addresses of machines from the property file
		Properties prop = new Properties();
		FileInputStream fis = new FileInputStream("./boltdb.prop");
		prop.load(fis);
		fis.close();
		ClientArgs clientArgs = new ClientArgs();
		
		//Create an object to store the key,value and other grep options
		StringBuilder options = new StringBuilder();
		for (int j = 0; j < args.length; j++) {
			if (args[j].equals("-key")) {
				clientArgs.setKeyRegExp(args[++j]);
			} else if (args[j].equals("-value")) {
				clientArgs.setValRegExp(args[++j]);
			} else {
				options.append(args[j]);
				options.append(" ");
			}
		}
		clientArgs.addOption(options.toString());
		if (clientArgs.getKeyRegExp().isEmpty()
				&& clientArgs.getValRegExp().isEmpty()) {
			System.out.println("ERROR : Both key and value parameters missing");
		}
		
		String[] addresses = prop.getProperty("machines.address").split(",");
		//Create and start a list of client threads. Each thread takes server address and port as input.
		LinkedList<Thread> clientThreads = new LinkedList<Thread>();
		for (int i = 0; i < addresses.length; i++) {
			String[] hostPort = addresses[i].split(":");
			InetAddress address = InetAddress.getByName(hostPort[0]);
			Thread newThread = new LogQuerierClientThread(address,
					Integer.parseInt(hostPort[1]), clientArgs);
			newThread.start();
			clientThreads.add(newThread);
		}
		//Each thread would create a temp file to store the grep output
		//from the server it talked to. This part of the code keeps track
		//completed threads. Once a thread completes,we open up the temp file
		//and print it out to the console.
		int completedThreads = 0;
		while (completedThreads != addresses.length) {
			for (int i = 0; i < clientThreads.size(); i++) {
				if (!clientThreads.get(i).isAlive()) {
					completedThreads++;
					printOutput(clientThreads.get(i));
					clientThreads.remove(i);
				}
			}
		}

	}

	//This method prints out the temp file created by the thread.
	private static void printOutput(Thread t) throws IOException {
		LogQuerierClientThread lct = (LogQuerierClientThread) t;
		File outputFile = new File("output-"
				+ lct.getAddress().getHostAddress() + "." + lct.getPort());
		if (outputFile.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(outputFile));
			String line;
			boolean atLeastOneLinePresent = false;
			while ((line = br.readLine()) != null) {
				if (!atLeastOneLinePresent) {
					System.out.println();
					System.out.println("Logs from " + lct.getAddress() + ":"
							+ lct.getPort());
					atLeastOneLinePresent = true;
				}
				System.out.println(line);
			}
			if(!atLeastOneLinePresent) System.out.println("No logs from "+lct.getAddress() + ":"
							+ lct.getPort());
			br.close();
			outputFile.delete();
			System.out.println();
		} 
	}
}
