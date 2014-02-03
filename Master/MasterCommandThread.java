package Master;

import java.util.ArrayList;
import java.util.Scanner;

import Util.*;
import Example.*;

public class MasterCommandThread extends Thread{
	Master master;
	public MasterCommandThread(Master _master) {
		master = _master;
	}
	public void run()
	{
		Scanner s = new Scanner(System.in);
		String line = null;
		while(true)
		{
			line = s.nextLine();
			String[] strList = line.split(" ");
			
			if(strList[0].equals("start"))
			{
				String jobName = strList[1];
				MapReduceJob job = null;
				try {
					job = (MapReduceJob)Class.forName("Example." + jobName).newInstance();
				} catch (Exception e) {
					e.printStackTrace();
				}
				ArrayList<String> input = new ArrayList<String>();
				for(int i = 3; i < strList.length; i++)
				{
					input.add(strList[i]);
				}
				job.withInputFiles(input)
				.withOutputFile(strList[2]);
				try {
					master.newJob(job);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else if(strList[0].equals("quit") || strList[0].equals("exit"))
			{
				try {
					master.quit();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else if(strList[0].equals("list"))
			{
				if(strList[1].equals("slaves"))
				{
					master.listAllSlaves();
				}
				if(strList[1].equals("files"))
				{
					master.listAllFiles();
				}
				if(strList[1].equals("tasks"))
				{
					master.listAllTasks();
				}
			}
			else
			{
				System.out.println("Usage: ");
				System.out.println("start [job_name] [output_file_name] [input_files]");
				System.out.println("list [slaves | files | tasks]");
				System.out.println("quit | exit");
			}
		}
	}

}
