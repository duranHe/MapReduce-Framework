package DFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.List;

import Util.Message;
import Configuration.*;

public class DataNode {

	public DataNode() {
	}

	// two methods to converse int to bytes, and vice versa
	public static int b2i(byte[] b) {  
        int value = 0;  
        for (int i = 0; i < 4; i++) {  
            int shift = (4 - 1 - i) * 8;  
            value += (b[i] & 0x000000FF) << shift;  
        }  
        return value;  
    }
	
	public static byte[] i2b(int i) {  
        return new byte[]{  
                (byte) ((i >> 24) & 0xFF),  
                (byte) ((i >> 16) & 0xFF),  
                (byte) ((i >> 8) & 0xFF),  
                (byte) (i & 0xFF)  
        };  
    }

	// send a local file to another data node.
	public void sendFile(String fileName, OutputStream os) throws Exception
	{
		File file = new File(fileName);
		int fileLength = (int)(file.length());
		os.write(i2b(fileLength));		// first send the length of file
		FileInputStream fis = new FileInputStream(fileName);
		byte[] buffer = new byte[4096];
		int size;
		while((size = fis.read(buffer)) != -1)
		{
			os.write(buffer, 0, size);
		}
		fis.close();
	}
	
	// this is for Slave to call. when u want a file, ask for it~
	public void askForFile(String fileName) throws Exception
	{
		String targetIP = askNNForFileAddr(fileName);
		getFileFromDN(targetIP, fileName);
	}
	
	// nitify the namenode that it wants a file, and get IP address from it.
	public String askNNForFileAddr(String fileName) throws Exception
	{
		Socket socketNN = null;
		String masterIP = Configuration.masterIP;
		int masterPort = Configuration.masterPort;
		socketNN = new Socket(masterIP, masterPort);
		
		OutputStream osNN = socketNN.getOutputStream();
		InputStream isNN = socketNN.getInputStream();
		ObjectInputStream oisNN = new ObjectInputStream(isNN);
		ObjectOutputStream oosNN = new ObjectOutputStream(osNN);
		
		Message message = new Message();
		message.withOp("askForFile")
		.withFileName(fileName);
		oosNN.writeObject(message);
		oosNN.flush();
		Message response = (Message)oisNN.readObject();
		ArrayList<String> targetIPList = response.getIPAddr();
		if(targetIPList.size() > 1)
			System.out.println("More than 1 IP address received when asking for a file! Using the first one...");
		String targetIP = targetIPList.get(0);
		Thread.sleep(100);
		socketNN.close();
		return targetIP;
	}
	//contact a datanode, make it send a file to this datanode.
	public void getFileFromDN(String targetIP, String fileName) throws Exception
	{
		int dataNodePort = Configuration.slavePort;
		Socket socketDN = new Socket(targetIP, dataNodePort);
		
		OutputStream osDN = socketDN.getOutputStream();
		InputStream isDN = socketDN.getInputStream();
		//ObjectInputStream oisDN = new ObjectInputStream(isDN);
		ObjectOutputStream oosDN = new ObjectOutputStream(osDN);
		
		Message message2 = new Message();
		message2.withFileName(fileName)
		.withOp("getFile");
		oosDN.writeObject(message2);
		
		// start reading file length, then file content
		byte[] len = new byte[4];
		isDN.read(len);
		int fileLength = b2i(len);
		
		// add prefix "replica" for testing on my own machine
		FileOutputStream fos = new FileOutputStream(fileName);
		byte[] buffer = new byte[4096];
		int count = 0;
		while(count < fileLength)
		{
			int readSize = isDN.read(buffer);
			fos.write(buffer, 0, readSize);
			count += readSize;
		}
		fos.flush();
		fos.close();
		Thread.sleep(100);
		socketDN.close();
		System.out.println("File " + fileName + " successfully received from " + targetIP);
	}
	// used when user gives uploadFile command on this dataNode.
	// ois, oos should be the Streams between this dataNode(slave) and the NameNode(master)
	public void uploadFile(String fileName, ObjectInputStream ois, ObjectOutputStream oos, OutputStream os) throws Exception
	{
		InetAddress inet = InetAddress.getLocalHost();
	    String localIP = inet.getHostAddress();
		Message message = new Message();
		message.withOp("uploadFile")
		.withFileName(fileName)
		.withIPAddr(localIP);
		oos.writeObject(message);
		Message response = (Message)ois.readObject();
		ArrayList<String> IPList = response.getIPAddr();
		System.out.println("Got " + IPList.size() + " IP addresses from Master.");
		sendFile(fileName, os);
		
		for(String IPAddr : IPList)
		{
			int slavePort = Configuration.slavePort;
			Socket socketDN = new Socket(IPAddr, slavePort);
			OutputStream osDN = socketDN.getOutputStream();
			//InputStream isDN = socketDN.getInputStream();
			//ObjectInputStream oisDN = new ObjectInputStream(isDN);
			ObjectOutputStream oosDN = new ObjectOutputStream(osDN);
			
			message = new Message();
			message.withOp("uploadFileOnDN")
			.withFileName(fileName);
			oosDN.writeObject(message);
			// now it should start sending file.
			sendFile(fileName, osDN);
			
			Thread.sleep(100);
			socketDN.close();
		}
	}
	// used when some other dataNode wants to upload a file on this dataNode
	// "InputStream is" is the InputStream between these two dataNodes.
	public void receiveFile(String fileName, InputStream is) throws Exception
	{
		// start reading file length, then file content
		byte[] len = new byte[4];
		is.read(len);
		int fileLength = b2i(len);
				
		FileOutputStream fos = new FileOutputStream(fileName);
		byte[] buffer = new byte[4096];
		int count = 0;
		while(count < fileLength)
		{
			int readSize = is.read(buffer);
			fos.write(buffer, 0, readSize);
			count += readSize;
		}
		fos.flush();
		fos.close();
		
		// notify master that this file is written on this 
		String masterIP = Configuration.masterIP;
		int masterPort = Configuration.masterPort;
		Socket socket = new Socket(masterIP, masterPort);
		//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		InetAddress inet = InetAddress.getLocalHost();
	    String localIP = inet.getHostAddress();
		Message message = new Message();
		message.withOp("fileAddedToDFS")
		.withFileName(fileName)
		.withIPAddr(localIP);
		oos.writeObject(message);
		oos.flush();
		Thread.sleep(100);
		socket.close();
	}
	// read interface for DFS.
	// calling this method will cause file to be fetched to local FS
	public void readDFS(String fileName) throws Exception
	{
		askForFile(fileName);
	}
	// write interface for DFS.
	// calling this method will cause a local file to be written to DFS.
	// thus it is visible to all nodes in the system.
	public void writeDFS(String fileName) throws Exception
	{
		String masterIP = Configuration.masterIP;
		int masterPort = Configuration.masterPort;
		Socket socket = new Socket(masterIP, masterPort);
		OutputStream os = socket.getOutputStream();
		InputStream is = socket.getInputStream();
		ObjectInputStream ois = new ObjectInputStream(is);
		ObjectOutputStream oos = new ObjectOutputStream(os);
		uploadFile(fileName, ois, oos, os);
		Thread.sleep(100);
		socket.close();
	}
}
