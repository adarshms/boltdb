package edu.uiuc.boltdb.logquerier.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

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
		System.out.println();

		ClientArgs clientArgs = new ClientArgs();
		// TODO error cases needed;check for spaces,incorrect option
		for (int j = 0; j < args.length; j++) {
			if (args[j].equals("-key")) {
				clientArgs.setKeyRegExp(args[++j]);
			} else if (args[j].equals("-value")) {
				clientArgs.setValRegExp(args[++j]);
			}
		}

		if (clientArgs.getKeyRegExp().isEmpty()
				&& clientArgs.getValRegExp().isEmpty()) {
			throw new IllegalArgumentException();
		}

		String[] addresses = prop.getProperty("machines.address").split(",");
		for (int i = 0; i < addresses.length; i++) {
			String[] hostPort = addresses[i].split(":");
			// TODO error conditions
			InetAddress address = InetAddress.getByName(hostPort[0]);
			Thread clientThread = new LogQuerierClientThread(address,
					Integer.parseInt(hostPort[1]), clientArgs);
			clientThread.start();
		}

	}
}
