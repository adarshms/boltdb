package edu.uiuc.boltdb.logquerier.tests;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * This class performs the various steps needed to execute a unit test case. It has methods to perform -
 * 1) Log file generation
 * 2) Distritubed log querying
 * 3) Local grep on the files
 * 4) Compare output of distributed grep and local grep
 * 
 * The main method of this class accepts two kinds of arguments -
 * 1) The option "-generateLogs" is used to generate log files
 * 		* The log files will be generated in the current directory
 *      * The log files will be named machine.1.log, machine.2.log and machine.3.log
 * 2) The name of the unit test case. Eg- "unit_test_1"
 * 		* For the unit test case to be a valid unit test case, there has to be a folder with the name of the 
 * 		  unit test case under "unit_tests/". Eg- unit_tests/unit_test_1
 *      * The folder should have a properties file "unit_test.prop". The properties file shoud have and entry
 *        "key" with the value of a keyRegExp, and another entry "value" with the value of a valRegExp
 * @author adarshms
 *
 */
public class LogQuerierTest
{
	/**
	 * This method takes a keyRegExp and a valRegExp as arguments and performs the distributed log query
	 * (distributed grep) by calling the command ./dgrep and passing it the keyRegExp and valRegExp arguments.
	 * It stores the output of the dgrep command in the file "output_dist.txt" under the unitTestName directory.
	 * @param keyRegExp
	 * @param valRegExp
	 * @param unitTestName
	 * @return
	 */
	public boolean executeDistributedQuery(String keyRegExp, String valRegExp, String unitTestName)
	{
		System.out.println("Executing Distributed query for unit test cast : " + unitTestName);
		ProcessBuilder pb = null;
		try
		{
			if(keyRegExp != "" && valRegExp != "")
				pb = new ProcessBuilder("./dgrep", "-key", keyRegExp, "-value", valRegExp);
			else if(keyRegExp != "")
				pb = new ProcessBuilder("./dgrep", "-key", keyRegExp);
			else if(valRegExp != "")
				pb = new ProcessBuilder("./dgrep", "-value", valRegExp);
			//pb.redirectOutput(new File("unit_tests/" + unitTestName + "/output_dist.txt"));
			Process ps = pb.start();
			BufferedReader is = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			BufferedWriter os = new BufferedWriter(new FileWriter("unit_tests/" + unitTestName + "/output_dist.txt"));
			String line;
			while ((line = is.readLine()) != null) 
			{
			    os.write(line + "\n");
			}
			if(ps.waitFor() != 0)
			{
				System.out.println("ERROR : Error executing distributed query for : " + unitTestName);
				is.close();
				os.close();
				return false;
			}
			else
			{
				is.close();
				os.close();
				return true;
			}
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception executing distributed query for : " + unitTestName);
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * This method cleans up the output of distributed grep and sorts it so that it can be easily compared
	 * with the output of local grep. It removes empty lines and lines such as "Logs from <ip_addresss>". 
	 * It works on the file "output_dist.txt" inside unitTestName directory  and saves the output in the 
	 * file "output_dist_clean.txt" under the same directory.
	 * @param unitTestName
	 * @return
	 */
	public boolean cleanUpDistributedOutput(String unitTestName)
	{
		System.out.println("Cleaning output of Distributed query for unit test cast : " + unitTestName);
		ProcessBuilder pb = null;
		try
		{
			List<String> commands = new ArrayList<String>();
            commands.add("/bin/sh");
            commands.add("-c");
            commands.add("grep -v -E '(^Logs from .*|^$)' unit_tests/" + unitTestName + "/output_dist.txt | sort");
			pb = new ProcessBuilder(commands);
			//pb.redirectOutput(new File("unit_tests/" + unitTestName + "/output_dist_clean.txt"));
			Process ps = pb.start();
			BufferedReader is = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			BufferedWriter os = new BufferedWriter(new FileWriter("unit_tests/" + unitTestName + "/output_dist_clean.txt"));
			String line;
			while ((line = is.readLine()) != null) 
			{
			    os.write(line + "\n");
			}
			if(ps.waitFor() != 0)
			{
				is.close();
				os.close();
				System.out.println("ERROR : Error cleaning output of distributed query for : " + unitTestName);
				return false;
			}
			else
			{
				is.close();
				os.close();
				
				return true;
			}
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception cleaning output of distributed query for : " + unitTestName);
			return false;
		}
	}
	
	/**
	 * This method takes keyRegExp and valRegExp as arguments and executes the unix grep command 
	 * on all the files locally and stores the output in the file "output_local.txt" inside the 
	 * unitTestName directory. It then sorts this file so that it can be easily compared with 
	 * the output of distributed grep and saves the sorted output in the file "output_local_clean.txt"
	 * under the same directory.
	 * @param keyRegExp
	 * @param valRegExp
	 * @param unitTestName
	 * @return
	 */
	public boolean executeLocalQuery(String keyRegExp, String valRegExp, String unitTestName)
	{
		System.out.println("Executing local query for unit test cast : " + unitTestName);
		ProcessBuilder pb = null;
		try
		{
			for(int i=1; i<4; i++)
			{
				List<String> commands = new ArrayList<String>();
	            commands.add("/bin/sh");
	            commands.add("-c");
				if(keyRegExp != "" && valRegExp != "")
					commands.add("grep -E '(" + keyRegExp + ".*:.*" + valRegExp + ")' machine." + i + ".log");
				else if(keyRegExp != "")
					commands.add("grep -E '(" + keyRegExp + ".*:)' machine." + i + ".log");
				else if(valRegExp != "")
					commands.add("grep -E '(:.*" + valRegExp + ")' /machine." + i + ".log");
				pb = new ProcessBuilder(commands);
				//pb.redirectOutput(Redirect.appendTo(new File("unit_tests/" + unitTestName + "/output_local.txt")));
				Process ps = pb.start();
				BufferedReader is = new BufferedReader(new InputStreamReader(ps.getInputStream()));
				BufferedWriter os = new BufferedWriter(new FileWriter("unit_tests/" + unitTestName + "/output_local.txt", true));
				String line;
				while ((line = is.readLine()) != null) 
				{
				    os.write(line + "\n");
				}
				ps.waitFor();
				is.close();
				os.close();
			}
			List<String> commands = new ArrayList<String>();
            commands.add("/bin/sh");
            commands.add("-c");
            commands.add("sort unit_tests/" + unitTestName + "/output_local.txt");
            pb = new ProcessBuilder(commands);
            //pb.redirectOutput(new File("unit_tests/" + unitTestName + "/output_local_clean.txt"));
            Process ps = pb.start();
			BufferedReader is = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			BufferedWriter os = new BufferedWriter(new FileWriter("unit_tests/" + unitTestName + "/output_local_clean.txt"));
			String line;
			while ((line = is.readLine()) != null) 
			{
			    os.write(line + "\n");
			}
			ps.waitFor();
			is.close();
			os.close();
			return true;
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception executing local query for : " + unitTestName);
			return false;
		}
	}
	
	/**
	 * This method compares the output of distributed grep and local grep for unitTestName.
	 * It reads the files "output_dist_clean.txt" and "output_local_clean.txt" inside the folder
	 * unitTestName and compares them line by line. It returns true if the two files are identical, 
	 * else returns false
	 * @param unitTestName
	 * @return
	 */
	public boolean compareOutputs(String unitTestName)
	{
		System.out.println("Comparing the outputs of Distributed query and local query for unit test cast : " + unitTestName);
		BufferedReader br1 = null;
		BufferedReader br2 = null;
		try
		{
			br1 = new BufferedReader(new FileReader("unit_tests/" + unitTestName + "/output_dist_clean.txt"));
			br2=new BufferedReader(new FileReader("unit_tests/"  + unitTestName + "/output_local_clean.txt"));
			String line1 = null, line2 = null;
			while(true)
			{
				line1 = br1.readLine();
				line2 = br2.readLine();
				if(line1 == null && line2 == null)
				{
					br1.close();
					br2.close();
					return true;
				}
				if(line1 == null || line2 == null)
				{
					break;
				}
				if(!line1.equals(line2))
					break;
			}
			br1.close();
			br2.close();
			return false;
		}
		catch(Exception e)
		{
			try 
			{
				br1.close();
				br2.close();
			} 
			catch (IOException e1) 
			{
				e1.printStackTrace();
			}
			System.out.println("ERROR : Exeception comparing outputs : " + unitTestName);
			return false;
		}
	}
	
	/**
	 * This method generates log files  dynamically. It reads the raw text data from the file 
	 * "raw_text_data.txt" under the folder "unit_tests/" to generate the log files. It generates 3
	 * log files with the name machine.1.log, machine.2.log and machine.3.log in the current folder. 
	 * Each log file will have 10,000 log lines. Each log line will have a key and a value separated by 
	 * the delimiter ":".
	 * The log files are generated such that -
	 * 1) 70% of the lines have the the key "FREQKEY" and value "FREQVAL"
	 * 2) 25% of the lines have the key "NSFREQKEY" and value "NSFREQVAL"
	 * 3) 4% of the lines have the key "RAREKEY" and value "RAREVAL"
	 * 4) 1% of the lines have the key "VRAREKEY" and value "VRAREVAL"
	 */
	public void genrateLogFiles() 
	{
		try 
		{
			BufferedReader br=new BufferedReader(new FileReader("unit_tests/raw_text_data.txt"));
			BufferedWriter bwData = null;
			char[] cbuf = new char[64];
			
			int lineCount = 0;
			int fileCount = 1;
			String logLine = "";
			Random generator = new Random();
			bwData = new BufferedWriter(new FileWriter("machine." + fileCount + ".log", true));
			while(br.read(cbuf, 0, 64) > 0)
			{
				int randomNumberForKey = generator.nextInt(100);
				if(randomNumberForKey < 70)
				{
					logLine = "FREQKEY";
				}
				else if(randomNumberForKey < 95)
				{
					logLine = "NSFREQKEY";
				}
				else if(randomNumberForKey < 99)
				{
					logLine = "RAREKEY";
				}
				else
				{
					logLine = "VRAREKEY";
				}
				logLine += ":";
				String line = new String(cbuf);
				line = line.replaceAll(":", "");
				logLine += line;
				if(randomNumberForKey < 70)
				{
					logLine += " FREQVAL";
				}
				else if(randomNumberForKey < 95)
				{
					logLine += " NSFREQVAL";
				}
				else if(randomNumberForKey < 99)
				{
					logLine += " RAREVAL";
				}
				else
				{
					logLine += " VRAREVAL";
				}
				
				logLine += " NODE##1";
				if(lineCount == 10000)
				{
					lineCount = 0;
					fileCount++;
					if(fileCount > 3)
						break;
					bwData.close();
					bwData=new BufferedWriter(new FileWriter("machine." + fileCount + ".log", true));
				}
				bwData.write(logLine + "\n");
				lineCount++;
			}
			br.close();
			bwData.close();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}		
	}
	
	
	/**
	 * The main method takes only one argument. If the user enters -
	 * 1) "-generateLogs", the program generates log files by calling the generateLogFiles() method
	 * 2) <unit_test_name> Eg- "unit_test_1". It executes the specified unit test.
	 * @param args
	 */
	public static void main(String[] args)
	{
		if (args.length != 1)
			System.out.println("Please specify a Unit Test Case name");
		else
		{
			LogQuerierTest lqTest = new LogQuerierTest();
			if(args[0].compareTo("-generateLogs") == 0)
			{
				lqTest.genrateLogFiles();
				return;
			}
			if((new File("unit_tests/" + args[0])).exists())
			{
				String unitTestName = args[0];
				try
				{
					Properties prop = new Properties();
					FileInputStream fis = new FileInputStream("unit_tests/" + unitTestName + "/unit_test.prop");
					prop.load(fis);
					fis.close();
					
					if(lqTest.executeDistributedQuery(prop.getProperty("key"), prop.getProperty("value"), unitTestName) &&
					lqTest.cleanUpDistributedOutput(unitTestName) &&
					lqTest.executeLocalQuery(prop.getProperty("key"), prop.getProperty("value"), unitTestName) &&
					lqTest.compareOutputs(unitTestName))
						System.out.println("Unit Test Case : " + unitTestName + " was SUCCESSFULLY executed");
					else
						System.out.println("Unit Test Case : " + unitTestName + " FAILED");
				}
				catch(IOException ioe)
				{
					System.out.println("ERROR : Exeception while executing unit test : " + unitTestName);
				}
			}
			else
			{
				System.out.println("ERROR : Invalid Unit Test Case name. Please enter a valid Unit Test Case name");
			}
		}
	}
}