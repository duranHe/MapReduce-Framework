package Util;

public class MapTask implements Task{
	private String inputFileName;
	private String outputFileName;
	private MapReduceJob job;
	private FilePartition fp;
	private int taskID;
	private String phase;
	public MapTask() {
	}
	public MapTask withInputFile(String _fileName)
	{
		inputFileName = _fileName;
		return this;
	}
	public MapTask withOutputFile(String _fileName)
	{
		outputFileName = _fileName;
		return this;
	}
	public MapTask withJob(MapReduceJob _job)
	{
		job = _job;
		return this;
	}
	public MapTask withPartition(FilePartition _fp)
	{
		fp = _fp;
		return this;
	}
	public MapTask withTaskID(int id)
	{
		taskID = id;
		return this;
	}
	public MapTask withPhase(String _phase)
	{
		phase = _phase;
		return this;
	}
	public String getInputFile()
	{
		return inputFileName;
	}
	public String getOutputFile()
	{
		return outputFileName;
	}
	public MapReduceJob getJob()
	{
		return job;
	}
	public FilePartition getPartition()
	{
		return fp;
	}
	public int getTaskID()
	{
		return taskID;
	}
	public String getPhase()
	{
		return phase;
	}
}
