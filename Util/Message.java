package Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Message implements Serializable{
	private String opName;
	private String fileName;
	private ArrayList<String> IPAddrList;
	private Task task;
	private int ID;
	public Message() {
		IPAddrList = new ArrayList<String>();
	}
	public Message withOp(String _opName)
	{
		opName = _opName;
		return this;
	}
	public Message withFileName(String _fileName)
	{
		fileName = _fileName;
		return this;
	}
	public Message withIPAddr(String _IPAddr)
	{
		IPAddrList.add(_IPAddr);
		return this;
	}
	public Message withIPAddrList(ArrayList<String> _newIPList)
	{
		IPAddrList.addAll(_newIPList);
		return this;
	}
	public Message withTask(Task _task)
	{
		task = _task;
		return this;
	}
	public Message withID(int _ID)
	{
		ID = _ID;
		return this;
	}
	public String getOpName()
	{
		return opName;
	}
	public String getFileName()
	{
		return fileName;
	}
	public ArrayList<String> getIPAddr()
	{
		return IPAddrList;
	}
	public Task getTask()
	{
		return task;
	}
	public int getID()
	{
		return ID;
	}
}
