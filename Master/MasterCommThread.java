package Master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import Configuration.*;

// this is the heartbeat thread on the master side. 
// just periodically accept and close
public class MasterCommThread extends Thread
{
	private ServerSocket ss;
	private int slaveID;
	private Master master;
	
	public MasterCommThread(Master _master, int id) throws Exception
	{
		ss = new ServerSocket(Configuration.masterPort + id * 2 + 1);
		slaveID = id;
		master = _master;
	}
	
	public void run()
	{
		try 
		{
			ss.setSoTimeout(Configuration.heartBeatTimeout);
			while(true)
			{
				Socket socket = ss.accept();
				socket.close();
			} 
		}
		catch (IOException e) 
		{
			System.out.println("slave " + slaveID +  " died");
			try {
				master.onSlaveFailure(slaveID);
			} catch (Exception e2) {
				e2.printStackTrace();
			}
			try 
			{
				ss.close();
			}
			catch (IOException e1) 
			{
				e1.printStackTrace();
			}
		}
	}
}
