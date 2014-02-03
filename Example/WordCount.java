package Example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import Util.*;

public class WordCount implements MapReduceJob{

	ArrayList<String> inputFiles;
	String outputFile;
	String jobName;
	int jobID;
	public WordCount() {
		jobName = "WordCount";
	}
	public WordCount withInputFiles(ArrayList<String> _inputFiles)
	{
		inputFiles = _inputFiles;
		return this;
	}
    public WordCount withOutputFile(String _outputFile)
    {
    	outputFile = _outputFile;
    	return this;
    }
    public WordCount withJobID(int id)
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
    
    public List<String[]> map(String key, String value) throws Exception
    {
    	String[] words = value.split(" ");
        List<String[]> out = new ArrayList<String[]>(words.length);
        for (String s : words) {
                out.add(new String[] {s, "1"});
                	//System.out.println(s);
                	//Thread.sleep(200);
        }
        
        return out;
    }
    
    public String reduce(String key, List<String> vals)
    {
    	int sum = 0;
    	for(String line : vals)
    	{
    		sum += Integer.parseInt(line);
    	}
    	String result = key + "\t" + sum + "\n";
    	return result;
    }

}
