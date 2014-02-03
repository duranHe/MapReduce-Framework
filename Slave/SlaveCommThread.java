package Slave;

import java.net.Socket;

import Configuration.*;

// this is the heartbeat thread on the slave side
public class SlaveCommThread extends Thread
{
	private int slaveID;
	
	public SlaveCommThread(int id)
	{
		slaveID = id;
	}
	
	public void run()
	{
		while(true)
		{
			try 
			{
				sleep(Configuration.heartBeatInterval);
				Socket socket = new Socket(Configuration.masterIP, Configuration.masterPort + slaveID * 2 + 1);
				socket.close();
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}
}
