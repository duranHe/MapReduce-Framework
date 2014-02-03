package Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// the interface of user-defined MapReduce job
public interface MapReduceJob extends Serializable{
	public MapReduceJob withInputFiles(ArrayList<String> inputFiles);
    public MapReduceJob withOutputFile(String outputFile);
    public MapReduceJob withJobID(int id);
    public ArrayList<String> getInputFiles();
    public String getOutputFile();
    public int getPartitionSize();
    public int getRecordSize();
    public String getJobName();
    public int getJobID();
    
    public List<String[]> map(String key, String value) throws Exception;
    
    public String reduce(String key, List<String> vals);
    
    

}
