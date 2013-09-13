package edu.uiuc.boltdb.logquerier.tests;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.*;


public class LogQuerierTest
{
	public void generateLogFiles()
	{
		
	}
	
	public boolean distributeLogFile(String logFilePath)
	{
		try
		{
			Scp scp = new Scp();
			scp.setLocalFile(logFilePath);
			scp.setRemoteTodir("sudhind2:@linux.ews.illinois.edu:" + "/home/sudhind2/workspace/cs425/mp1/");
	        scp.setProject(new Project());
	        scp.setTrust(true);
	        scp.execute();
	        return true;
		}
		catch(Exception e)
		{
			return false;
		}
	}
	
	public boolean executeDistributedQuery(String keyRegExp, String valRegExp, String unitTestName)
	{
		ProcessBuilder pb = null;
		try
		{
			if(keyRegExp != "" && valRegExp != "")
				pb = new ProcessBuilder("./dgrep", "-key", keyRegExp, "-value", valRegExp);
			else if(keyRegExp != "")
				pb = new ProcessBuilder("./dgrep", "-key", keyRegExp);
			else if(valRegExp != "")
				pb = new ProcessBuilder("./dgrep", "-value", valRegExp);
			pb.redirectOutput(new File("unit_tests/" + unitTestName + "/output_dist.txt"));
			Process ps = pb.start();
			if(ps.waitFor() != 0)
			{
				System.out.println("ERROR : Error executing distributed query for : " + unitTestName);
				return false;
			}
			else
				return true;
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception executing distributed query for : " + unitTestName);
			return false;
		}
	}
	
	public boolean cleanUpDistributedOutput(String unitTestName)
	{
		ProcessBuilder pb = null;
		try
		{
			List<String> commands = new ArrayList<String>();
            commands.add("/bin/sh");
            commands.add("-c");
            commands.add("grep -v -E '(^Logs from .*|^$)' unit_tests/" + unitTestName + "/output_dist.txt" + " | sort");
			pb = new ProcessBuilder(commands);
			pb.redirectOutput(new File("unit_tests/" + unitTestName + "/output_dist_clean.txt"));
			Process ps = pb.start();
			if(ps.waitFor() != 0)
			{
				System.out.println("ERROR : Error cleaning output of distributed query for : " + unitTestName);
				return false;
			}
			else
				return true;
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception cleaning output of distributed query for : " + unitTestName);
			return false;
		}
	}
	
	public boolean executeLocalQuery(String keyRegExp, String valRegExp, String unitTestName)
	{
		ProcessBuilder pb = null;
		try
		{
			for(int i=1; i<4; i++)
			{
				List<String> commands = new ArrayList<String>();
	            commands.add("/bin/sh");
	            commands.add("-c");
				if(keyRegExp != "" && valRegExp != "")
					commands.add("grep -E '(" + keyRegExp + ".*:.*" + valRegExp + ")' unit_tests/" + unitTestName + "/machine." + i + ".log");
				else if(keyRegExp != "")
					commands.add("grep -E '(" + keyRegExp + ".*:)' unit_tests/" + unitTestName + "/machine." + i + ".log");
				else if(valRegExp != "")
					commands.add("grep -E '(:.*" + valRegExp + ")' unit_tests/" + unitTestName + "/machine." + i + ".log");
				pb = new ProcessBuilder(commands);
				pb.redirectOutput(Redirect.appendTo(new File("unit_tests/" + unitTestName + "/output_local.txt")));
				Process ps = pb.start();
				ps.waitFor();
			}
			List<String> commands = new ArrayList<String>();
            commands.add("/bin/sh");
            commands.add("-c");
            commands.add("sort unit_tests/" + unitTestName + "/output_local.txt > unit_tests/" + unitTestName + "/output_local.txt");
			return true;
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception executing local query for : " + unitTestName);
			return false;
		}
	}
	
	public boolean compareOutputs(String unitTestName)
	{
		ProcessBuilder pb = null;
		try
		{
			List<String> commands = new ArrayList<String>();
	        commands.add("/bin/sh");
	        commands.add("-c");
	        commands.add("diff /unit_tests/" + unitTestName + "/output_dist_clean.txt /unit_tests/" + unitTestName + "/output_local.txt");
			pb = new ProcessBuilder(commands);
			pb.redirectOutput(new File("unit_tests/" + unitTestName + "/output_dist_clean.txt"));
			Process ps = pb.start();
			BufferedReader is = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			String line;
			if(is.readLine() != null) 
			{
			    return false;
			}
			return true;
		}
		catch(Exception e)
		{
			System.out.println("ERROR : Exeception comparing outputs : " + unitTestName);
			return false;
		}
	}
	
	public static void main(String[] args)
	{
		LogQuerierTest lqTest = new LogQuerierTest();
		lqTest.executeDistributedQuery("because", "tshirts", "unit_test_1");
		lqTest.cleanUpDistributedOutput("unit_test_1");
		lqTest.executeLocalQuery("because", "tshirts", "unit_test_1");
	}
}