package Util;

import java.util.ArrayList;


public class ReduceTask implements Task{
	private ArrayList<String> inputFiles;
	private String outputFileName;
	private MapReduceJob job;
	private int taskID;
	private String phase;
	private int reducerID;
	public ReduceTask() {
	}
	public ReduceTask withInputFiles(ArrayList<String> _inputFiles)
	{
		inputFiles = _inputFiles;
		return this;
	}
	public ReduceTask withOutputFile(String _fileName)
	{
		outputFileName = _fileName;
		return this;
	}
	public ReduceTask withJob(MapReduceJob _job)
	{
		job = _job;
		return this;
	}
	public ReduceTask withTaskID(int id)
	{
		taskID = id;
		return this;
	}
	public ReduceTask withPhase(String _phase)
	{
		phase = _phase;
		return this;
	}
	public ReduceTask withReducerID(int _reducerID)
	{
		reducerID = _reducerID;
		return this;
	}
	public ArrayList<String> getInputFiles()
	{
		return inputFiles;
	}
	public String getOutputFile()
	{
		return outputFileName;
	}
	public MapReduceJob getJob()
	{
		return job;
	}
	public int getTaskID()
	{
		return taskID;
	}
	public String getPhase()
	{
		return phase;
	}
	public int getReducerID()
	{
		return reducerID;
	}
}
