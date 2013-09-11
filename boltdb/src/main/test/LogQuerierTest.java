import java.io.File;

import org.junit.Test;

import junit.framework.TestCase;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.*;


public class LogQuerierTest extends TestCase {
	
	@Test
	public void testPattern1()
	{
		//assertEquals(true, distributeLogFile("/home/adarshms/mytestdoc"));
		assertEquals(true, executeDistributedQuery("", "", ""));
	}
	
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
	
	public boolean executeDistributedQuery(String keyRegExp, String valRegExp, String outputFile)
	{
		try
		{
			SSHExec ssh = new SSHExec();
			ssh.setCommand("ls -l /home/sudhind2/");
			ssh.setHost("linux.ews.illinois.edu");
			ssh.setUsername("sudhind2");
			ssh.setPassword("");
			ssh.setOutput(new File("/home/adarshms/sshoutput.txt"));
			ssh.setProject(new Project());
			ssh.setTrust(true);
			ssh.execute();
			return true;
		}
		catch(Exception e)
		{
			return false;
		}
	}
	
	public boolean executeLocalQuery(String keyRegExp, String valRegExp, String outputFile)
	{
		return false;
	}
	
	public boolean compareOutputs(String dQueryOutput, String lQueryOutput)
	{
		return false;
	}
}


//TODO : Push log files to servers

//TODO : Run command line dgrep with options 
//TODO : Read output of dgrep
//TODO : Run grep on local log files and generate output
//TODO : Compare two outputs
/*
192.17.11.102
192.17.11.103
192.17.11.105
192.17.11.104
*/