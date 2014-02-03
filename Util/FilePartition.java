package Util;

import java.io.Serializable;

public class FilePartition implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3945423547239942220L;
	private String fileName;
	private int size;
	private int index;
	
	public FilePartition(String name, int i, int s)
	{
		fileName = name;
		index = i;
		size = s;
	}
	
	public String getFileName()
	{
		return fileName;
	}
	
	public int getPartitionIndex()
	{
		return index;
	}
	
	public int getPartitionSize()
	{
		return size;
	}
}
