package Slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import DFS.DataNode;
import Configuration.*;
import Util.*;

public class Slave {
	DataNode dataNode;
	static String localIP;
	static int slaveID;
	HashMap<Integer, ArrayList<String>> tempFiles;
	public Slave() throws Exception {
		dataNode = new DataNode();
		InetAddress inet = InetAddress.getLocalHost();
	    localIP = inet.getHostAddress();
	    tempFiles = new HashMap<Integer, ArrayList<String>>();
	}
	public void doSlave() throws Exception
	{
		SlaveCommandThread slaveCommandThread = new SlaveCommandThread(this);
		slaveCommandThread.start();
		
		bindToMaster();
		
		int listenPort = Configuration.slavePort;
		ServerSocket ss = new ServerSocket(listenPort);
		Socket listenSocket;
		while((listenSocket = ss.accept()) != null)
		{
			OutputStream osSocketListen = listenSocket.getOutputStream();
			InputStream isSocketListen = listenSocket.getInputStream();
			//ObjectOutputStream oosListen = new ObjectOutputStream(osSocketListen);
			ObjectInputStream oisListen = new ObjectInputStream(isSocketListen);
			
			Message message = (Message)oisListen.readObject();
			System.out.println("Message received: " + message.getOpName());
			
			// some slave wants to get a file from this node
			if(message.getOpName().equals("getFile"))
			{
				String fileName = message.getFileName();
				System.out.println("Somebody wants to get the file " + fileName + " from here...");
				dataNode.sendFile(fileName, osSocketListen);
			}
			
			// master gives this node a map task
			if(message.getOpName().equals("giveMapTask"))
			{
				MapTask task = (MapTask)message.getTask();

				MapTaskThread mtt = new MapTaskThread(dataNode, task, this);
				mtt.start();
			}
			
			// master gives this node a reduce task
			if(message.getOpName().equals("giveReduceTask"))
			{
				ReduceTask task = (ReduceTask)message.getTask();
				
				ReduceTaskThread rtt = new ReduceTaskThread(dataNode, task, this);
				rtt.start();
			}
			
			// a file is to be uploaded on this slave 
			if(message.getOpName().equals("uploadFileOnDN"))
			{
				String fileName = message.getFileName();
				System.out.println("Somebody wants to upload a file " + fileName + " here...");
				dataNode.receiveFile(fileName, isSocketListen);
			}
			
			// a reducer wants to get the output of a mapper
			if(message.getOpName().equals("fetchFileFromMapper"))
			{
				String fileName = message.getFileName();
				ArrayList<String> ipList = message.getIPAddr();
				String targetIP = ipList.get(0);
				if(!targetIP.equals(localIP))
					dataNode.getFileFromDN(targetIP, fileName);
			}
			
			// master wants to quit
			if(message.getOpName().equals("quit"))
			{
				System.out.println("Terminating...");
				System.exit(0);
			}
			
			// clean up temporary files after a job is done
			if(message.getOpName().equals("cleanUp"))
			{
				int jobID = message.getID();
				System.out.println("Cleaning up for job " + jobID);
				cleanUp(jobID);
			}
		}
	}
	
	// upon starting, bind to master
	public void bindToMaster() throws Exception
	{
	    Message initMessage = new Message();
	    initMessage.withOp("bindSlave")
	    .withIPAddr(localIP);
	    Socket initSocket = new Socket(Configuration.masterIP, Configuration.masterPort);
	    ObjectInputStream ois = new ObjectInputStream(initSocket.getInputStream());
	    ObjectOutputStream oos = new ObjectOutputStream(initSocket.getOutputStream());
	    oos.writeObject(initMessage);
	    Message res = (Message)ois.readObject();
	    if(res.getOpName().equals("ACK"))
	    {
	    	System.out.println("Bind to master done");
	    	slaveID = res.getID();
			SlaveCommThread SlaveCommThread = new SlaveCommThread(slaveID);
			SlaveCommThread.start();
	    }
	    Thread.sleep(100);
	    initSocket.close();
	}
	
	// this is the processing method as soon as user gives "upload" command
	public void uploadFile(String fileName) throws Exception
	{
		String masterIP = Configuration.masterIP;
		int masterPort = Configuration.masterPort;
		Socket socket = new Socket(masterIP, masterPort);
		OutputStream os = socket.getOutputStream();
		InputStream is = socket.getInputStream();
		ObjectInputStream ois = new ObjectInputStream(is);
		ObjectOutputStream oos = new ObjectOutputStream(os);
		dataNode.uploadFile(fileName, ois, oos, os);
		Thread.sleep(100);
		socket.close();
	}
	
	// when user types start command at slave side, send it to master
	public void letMasterStart(String commandLine) throws Exception
	{
		String[] strList = commandLine.split(" ");
		if(!strList[0].equals("start"))
		{
			System.out.println("This is not start, ignored...");
			return;
		}
		
		Socket socket = new Socket(Configuration.masterIP, Configuration.masterPort);
		
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		
		Message message = new Message();
		message.withOp("startJob")
		.withFileName(commandLine);
		oos.writeObject(message);
		Thread.sleep(100);
		socket.close();
	}
	
	// add to a temp file list, it is for the final clean up.
	public void addTempFiles(String fileName, int jobID)
	{
		synchronized(tempFiles)
		{
			ArrayList<String> fileList = tempFiles.get(jobID);
			if(fileList == null)
			{
				ArrayList<String> newList = new ArrayList<String>();
				newList.add(fileName);
				tempFiles.put(jobID, newList);
			}
			else
			{
				fileList.add(fileName);
				tempFiles.remove(jobID);
				tempFiles.put(jobID, fileList);
			}
		}
	}
	
	// clean up temp files
	public void cleanUp(int jobID)
	{
		ArrayList<String> fileList = tempFiles.get(jobID);
		for (String fileName : fileList)
		{
			System.out.println("Deleting file: " + fileName);
			try
			{
				File file = new File(fileName);
				file.delete();
			}
			catch(Exception e)
			{
				System.out.println(fileName + " may not exist, ignored...");
			}
		}
	}
	public void getFile(String fileName) throws Exception
	{
		dataNode.readDFS(fileName);
	}
}
