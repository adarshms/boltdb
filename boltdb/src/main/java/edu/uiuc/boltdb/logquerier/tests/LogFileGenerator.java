package edu.uiuc.boltdb.logquerier.tests;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class LogFileGenerator 
{
	private String rawTextDataFolder;
	private String logDataFolder;
	
	public LogFileGenerator(String rawTextDataFolder, String logDataFolder)
	{
		this.rawTextDataFolder = rawTextDataFolder;
		this.logDataFolder = logDataFolder;
	}
	
	public void generateLogFile()
	{
		try 
		{
			BufferedReader br=new BufferedReader(new FileReader(rawTextDataFolder + "raw_data_1.txt"));
			BufferedWriter bwData=new BufferedWriter(new FileWriter(logDataFolder + "machine.1.log", true));
			char[] cbuf = new char[64];
			
			String logLine = "";
			Random generator1 = new Random();
			Random generator2 = new Random();
			while(br.read(cbuf, 0, 64) > 0)
			{
				int randomNumberForKey = generator1.nextInt(100);
				int randomNumberForVal = generator2.nextInt(100);
				if(randomNumberForKey < 70)
				{
					logLine = "FREQKEY";
				}
				else if(randomNumberForKey < 95)
				{
					logLine = "NSFREQKEY";
				}
				else
				{
					logLine = "RAREKEY";
				}
				logLine += " - - ";
				String line = new String(cbuf);
				logLine += line;
				if(randomNumberForVal < 70)
				{
					logLine += " FREQVAL";
				}
				else if(randomNumberForVal < 95)
				{
					logLine += " NSFREQVAL";
				}
				else
				{
					logLine += " RAREVAL";
				}
				logLine += " NODE##1";
				bwData.write(logLine + "\n");
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
		
	public static void main(String args[])
	{
		LogFileGenerator index = new LogFileGenerator("/home/adarshms/academics/cs425/mp1/", "/home/adarshms/academics/cs425/mp1/");
		index.generateLogFile();
	}
}