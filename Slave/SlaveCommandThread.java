package Slave;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;


public class SlaveCommandThread extends Thread{
	Slave slave;
	public SlaveCommandThread(Slave _slave) {
		slave = _slave;
	}
	public void run()
	{
		Scanner s = new Scanner(System.in);
		String line = null;
		while(true)
		{
			line = s.nextLine();
			String[] strList = line.split(" ");
			if(strList[0].equals("upload"))
			{
				String fileName = strList[1];
				File file = new File(fileName);
				if(!file.exists())
				{
					System.out.println("File does not exist!");
					continue;
				}
				try {
					slave.uploadFile(fileName);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else if(strList[0].equals("start"))
			{
				try {
					slave.letMasterStart(line);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else if(strList[0].equals("get"))
			{
				String fileName = strList[1];
				try {
					slave.getFile(fileName);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Usage:");
				System.out.println("upload [file_name]");
				System.out.println("start [job_name] [output_file_name] [input_files]");
				System.out.println("get [file_name]");
			}
		}
	}
}
