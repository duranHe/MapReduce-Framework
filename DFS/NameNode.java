package DFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import Util.Message;


public class NameNode {
	HashMap<String, ArrayList<String>> fileName2SlaveIP;
	public NameNode() {
		fileName2SlaveIP = new HashMap<String, ArrayList<String>>();
	}
	
	// converse integer to bytes
	public static byte[] i2b(int i) {  
        return new byte[]{  
                (byte) ((i >> 24) & 0xFF),  
                (byte) ((i >> 16) & 0xFF),  
                (byte) ((i >> 8) & 0xFF),  
                (byte) (i & 0xFF)  
        };  
    }

	// find an IP where the specified file is stored, then return it to the node asking for it
	public void findIPAddr(String fileName, ObjectOutputStream oos) throws Exception
	{
		ArrayList<String> IPList = fileName2SlaveIP.get(fileName);
		int listSize = IPList.size();
		Random random = new Random();
		int index = random.nextInt(listSize);
		String IPAddr = IPList.get(index);
		Message response = new Message();
		response.withIPAddr(IPAddr);
		oos.writeObject(response);
		oos.flush();
	}
	
	// when a file is uploaded to DFS, update the metadata.
	public void addFileMetadata(String fileName, String IPAddr)
	{
		ArrayList<String> ipList = fileName2SlaveIP.get(fileName);
		if(ipList == null)
		{
			ipList = new ArrayList<String>();
			ipList.add(IPAddr);
			fileName2SlaveIP.put(fileName, ipList);
		}
		else
		{
			ipList.add(IPAddr);
			fileName2SlaveIP.remove(fileName);
			fileName2SlaveIP.put(fileName, ipList);
		}
		System.out.println("File " + fileName + " is added to node: " + IPAddr);
	}
	
	// list all the file information for the user.
	public void listAllFiles()
	{
		if(fileName2SlaveIP.isEmpty())
		{
			System.out.println("No files!");
			return;
		}
		Iterator iter = fileName2SlaveIP.entrySet().iterator();
		while (iter.hasNext())
    	{ 
    	    Map.Entry entry = (Map.Entry) iter.next();
    	    String fileName = (String)entry.getKey(); 
    	    ArrayList<String> slaveIPList = (ArrayList<String>)entry.getValue();
    	    for(String slaveIP : slaveIPList)
    	    	System.out.println("IP: " + slaveIP + "\tFile Name: " + fileName);
    	}
	}
}
