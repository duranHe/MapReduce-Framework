package Util;

import java.io.Serializable;


public interface Task extends Serializable{
	public Task withOutputFile(String fileName);
	public Task withJob(MapReduceJob job);
	public Task withTaskID(int id);
	public Task withPhase(String phase);
	public String getOutputFile();
	public MapReduceJob getJob();
	public int getTaskID();
	public String getPhase();
}
