package Example;

import java.util.ArrayList;
import java.util.List;

import Util.MapReduceJob;

public class LargestWord implements MapReduceJob {

	ArrayList<String> inputFiles;
	String outputFile;
	String jobName;
	int jobID;
	int largestNumber;
	String largestWord;
	public LargestWord() {
		jobName = "LargestWord";
		largestNumber = 0;
		largestWord = null;
	}
	public LargestWord withInputFiles(ArrayList<String> _inputFiles)
	{
		inputFiles = _inputFiles;
		return this;
	}
    public LargestWord withOutputFile(String _outputFile)
    {
    	outputFile = _outputFile;
    	return this;
    }
    public LargestWord withJobID(int id)
    {
    	jobID = id;
    	return this;
    }
    public ArrayList<String> getInputFiles()
    {
    	return inputFiles;
    }
    public String getOutputFile()
    {
    	return outputFile;
    }
    public int getPartitionSize()
    {
    	return 100;
    }
    public int getRecordSize()
    {
    	return 100;
    }
    public String getJobName()
    {
    	return jobName;
    }
    public int getJobID()
    {
    	return jobID;
    }

	public List<String[]> map(String key, String value) throws Exception {
		String[] words = value.split(" ");
        List<String[]> out = new ArrayList<String[]>(words.length);
        for (String s : words) {
                out.add(new String[] {s, "1"});
                //System.out.println(s);
                //Thread.sleep(200);
        }
        
        return out;
	}

	public String reduce(String key, List<String> vals) {
		int sum = 0;
    	for(String line : vals)
    	{
    		sum += Integer.parseInt(line);
    	}
    	String result = null;
    	if(sum > largestNumber)
    	{
    		largestWord = key;
    		largestNumber = sum;
    		result = key + "\t" + sum + "\n";
    	}
    	else
    		result = largestWord + "\t" + largestNumber + "\n";
    	return result;
	}

}
