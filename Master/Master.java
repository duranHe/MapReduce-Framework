package Master;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import DFS.NameNode;
import Util.*;
import Configuration.*;
import Example.*;

public class Master {
	// map slaveID to the IP address of the slave
	private HashMap<Integer, String> slaveID2Addr;
	// jobID to job. even if a job is done, I wanna keep it here.
	private HashMap<Integer, MapReduceJob> jobID2Job;
	// taskID to task. even if a task is done, I wanna keep it here.
	private HashMap<Integer, Task> taskID2Task;
	// jobID to all the tasks that belongs to this job
	private HashMap<Integer, ArrayList<Integer>> jobID2TaskID;
	// slaveID to all the tasks that currently running on this slave
	// this map is updated when a task is complete
	private HashMap<Integer, ArrayList<Integer>> slaveID2TaskID;
	// taskID to the slaveID where it runs on
	private HashMap<Integer, Integer> taskID2SlaveID;
	// list of running tasks.
	private ArrayList<Integer> runningTasks;
	// list of running jobs
	private ArrayList<Integer> runningJobs;
	
	
	private int globalSlaveID;
	private NameNode nameNode;
	private int globalJobID;
	private int globalTaskID;
	public Master() {
		globalSlaveID = 0;
		slaveID2Addr = new HashMap<Integer, String>();
		nameNode = new NameNode();
		globalJobID = 0;
		globalTaskID = 0;
		jobID2Job = new HashMap<Integer, MapReduceJob>();
		taskID2Task = new HashMap<Integer, Task>();
		jobID2TaskID = new HashMap<Integer, ArrayList<Integer>>();
		slaveID2TaskID = new HashMap<Integer, ArrayList<Integer>>();
		taskID2SlaveID = new HashMap<Integer, Integer>();
		runningTasks = new ArrayList<Integer>();
		runningJobs = new ArrayList<Integer>();
	}
	public void doMaster() throws Exception {
		InetAddress inet = InetAddress.getLocalHost();
	    String localIP = inet.getHostAddress();
	    System.out.println("Master IP: " + localIP);
	    
		MasterCommandThread masterCommandThread = new MasterCommandThread(this);
		masterCommandThread.start();
		
		ServerSocket ss = null;
		int listenPort = Configuration.masterPort;
		ss = new ServerSocket(listenPort);
		Socket socket;
		
		// loop to accept messages from slaves
		while((socket = ss.accept()) != null)
		{
			ObjectInputStream ois;
			ObjectOutputStream oos;
			OutputStream osSocket = socket.getOutputStream();
			InputStream isSocket = socket.getInputStream();
			oos = new ObjectOutputStream(osSocket);
			ois = new ObjectInputStream(isSocket);
			Message message = (Message) ois.readObject();
			System.out.println("Message received: " + message.getOpName());
			
			// a slave started and wants to participant to this system
			if(message.getOpName().equals("bindSlave"))
			{
				String IPAddr = message.getIPAddr().get(0);
				System.out.println("Found slave at address: " + IPAddr);
				synchronized(slaveID2Addr)
				{
					slaveID2Addr.put(globalSlaveID, IPAddr);
				}
								
				MasterCommThread MasterCommThread = new MasterCommThread(this, globalSlaveID);
				MasterCommThread.start();
				
				Message ack = new Message();
				ack.withOp("ACK")
				.withID(globalSlaveID);
				oos.writeObject(ack);
				
				globalSlaveID++;
			}
			
			// a slave is asking for the location of a file
			if(message.getOpName().equals("askForFile"))
			{
				String fileName = message.getFileName();
				nameNode.findIPAddr(fileName, oos);
			}
			
			// a slave wants to upload a file
			if(message.getOpName().equals("uploadFile"))
			{
				String fileName = message.getFileName();
				String fromIP = message.getIPAddr().get(0);
				ArrayList<String> IPList = findIPAddrForUpload(fileName, fromIP);
				Message response = new Message();
				response.withIPAddrList(IPList);
				oos.writeObject(response);
				
				byte[] len = new byte[4];
				isSocket.read(len);
				int fileLength = b2i(len);
				
				FileOutputStream fos = new FileOutputStream(fileName);
				byte[] buffer = new byte[4096];
				int count = 0;
				while(count < fileLength)
				{
					int readSize = isSocket.read(buffer);
					fos.write(buffer, 0, readSize);
					count += readSize;
				}
				fos.flush();
				fos.close();
			}
			
			// slave is notifying that a map task is done
			if(message.getOpName().equals("mapTaskDone"))
			{
				Task completedTask = message.getTask();
				taskComplete(completedTask);
			}
			
			// slave is notifying that a reduce task is done
			if(message.getOpName().equals("reduceTaskDone"))
			{
				Task completedTask = message.getTask();
				taskComplete(completedTask);
			}
			
			// a slave notifies the master that a file is added, so update metadata
			if(message.getOpName().equals("fileAddedToDFS"))
			{
				String fileName = message.getFileName();
				ArrayList<String> ipList = message.getIPAddr();
				String ip = ipList.get(0);
				nameNode.addFileMetadata(fileName, ip);
			}
			
			// slave notifies that user has entered start command on it.
			// we should still start the job on the master side.
			if(message.getOpName().equals("startJob"))
			{
				String commandLine = message.getFileName();
				String[] strList = commandLine.split(" ");
				String jobName = strList[1];
				MapReduceJob job = null;
				job = (MapReduceJob)Class.forName("Example." + jobName).newInstance();
				ArrayList<String> input = new ArrayList<String>();
				for(int i = 3; i < strList.length; i++)
				{
					input.add(strList[i]);
				}
				job.withInputFiles(input)
				.withOutputFile(strList[2]);
				newJob(job);
			}
		}
	}
	
	// start a new job. start distributing map tasks
	public void newJob(MapReduceJob job) throws Exception
	{
		ArrayList<String> inputFileList = job.getInputFiles();
		for(String file : inputFileList)
		{
			File f = new File(file);
			if(!f.exists())
			{
				System.out.println(file + " does not exist! Job cancelled!");
				return;
			}
		}
		
		// update information
		List<Task> mapTasks = new ArrayList<Task>();
		job.withJobID(globalJobID);
		synchronized(jobID2Job)
		{
			jobID2Job.put(globalJobID, job);
		}
		synchronized(runningJobs)
		{
			runningJobs.add(globalJobID);
		}
		globalJobID++;
		int curJobID = job.getJobID();
	
		ArrayList<String> fileList = job.getInputFiles();
		for (String fileName : fileList)
		{
            RecordReader reader = new RecordReader(fileName, job.getRecordSize());

            int totalRecords = reader.getRecordNum();

            for (int i = 0; i < totalRecords; i += Configuration.RECORD_PER_MAP)
            {
            	int numRecords;
            	if(i + Configuration.RECORD_PER_MAP > totalRecords)
            	{
            		numRecords = totalRecords - i;
            	}
            	else
            	{
            		numRecords = Configuration.RECORD_PER_MAP;
            	}

            	FilePartition fp = new FilePartition(fileName, i, numRecords);
            	MapTask mapTask = new MapTask();
            	mapTask.withPartition(fp)
            	.withJob(job)
            	.withInputFile(fileName)
            	.withTaskID(globalTaskID)
            	.withPhase("map");
            	synchronized(taskID2Task)
            	{
            		taskID2Task.put(globalTaskID, mapTask);
            	}
            	synchronized(runningTasks)
            	{
            		runningTasks.add(globalTaskID);
            	}
            	globalTaskID++;
            	int curTaskID = mapTask.getTaskID();
            	mapTask.withOutputFile("Job" + curJobID + "_Task" + curTaskID + "_MapOutput");
            	
            	// update the jobID2TaskID map with this task.
            	ArrayList<Integer> taskIDList = jobID2TaskID.get(curJobID);
            	if(taskIDList == null)
            	{
            		taskIDList = new ArrayList<Integer>();
            		taskIDList.add(curTaskID);
            		synchronized(jobID2TaskID)
            		{
            			jobID2TaskID.put(curJobID, taskIDList);
            		}
            	}
            	else
            	{
            		taskIDList.add(curTaskID);
            		synchronized(jobID2TaskID)
            		{
            			jobID2TaskID.remove(Integer.valueOf(curJobID));
            			jobID2TaskID.put(curJobID, taskIDList);
            		}
            	}
            	
            	mapTasks.add(mapTask);
            }
		}
		// distribute all the task in the list to slaves
		distributeTasks(mapTasks);
	}
	
	// start reduce tasks
	public void newReduce(MapReduceJob job) throws Exception
	{
		System.out.println("Start preparing and distributing reduce tasks for job " + job.getJobID() + " ...");
		List<Task> reduceTasks = new ArrayList<Task>(Configuration.reduceNum);
		for(int i = 0; i < Configuration.reduceNum; i++)
		{
			ReduceTask reduceTask = new ReduceTask();
			int jobID = job.getJobID();
			ArrayList<Integer> taskList;
			synchronized(jobID2TaskID)
			{
				taskList = jobID2TaskID.get(jobID);
			}
			ArrayList<String> inputFileNames = new ArrayList<String>();
			for(int taskID : taskList)
			{
				Task prevTask;
				synchronized(taskID2Task)
				{
					prevTask = taskID2Task.get(taskID);
				}
				if(prevTask.getPhase().equals("map"))
				{
					String outputFileName = "Job" + jobID + "_Task" + taskID + "_MapOutput_ForReducer" + i;
					inputFileNames.add(outputFileName);
				}
			}
			reduceTask.withInputFiles(inputFileNames)
			.withOutputFile(job.getOutputFile() + "_" + i)
			.withJob(job)
			.withPhase("reduce")
			.withTaskID(globalTaskID)
			.withReducerID(i);
			synchronized(taskID2Task)
			{
				taskID2Task.put(globalTaskID, reduceTask);
			}
			synchronized(runningTasks)
			{
				runningTasks.add(globalTaskID);
			}
			globalTaskID++;
			
			int curTaskID = reduceTask.getTaskID();
			
			// update jobID2TaskID
			ArrayList<Integer> taskIDList;
			synchronized(jobID2TaskID)
			{
				taskIDList = jobID2TaskID.get(jobID);
			}
        	if(taskIDList == null)
        	{
        		taskIDList = new ArrayList<Integer>();
        		taskIDList.add(curTaskID);
        		synchronized(jobID2TaskID)
    			{
        			jobID2TaskID.put(jobID, taskIDList);
    			}
        	}
        	else
        	{
        		taskIDList.add(curTaskID);
        		synchronized(jobID2TaskID)
    			{
        			jobID2TaskID.remove(Integer.valueOf(jobID));
        			jobID2TaskID.put(jobID, taskIDList);
    			}
        	}
        	
        	reduceTasks.add(reduceTask);
		}
		distributeTasks(reduceTasks);
	}
	
	// before starting reducer task, the slaves must have the mapper's output files.
	// they need to go to the corresponding slave and fetch it.
	public void notifyReducerFetchFile(int reducerSlaveID, ReduceTask curTask) throws Exception
	{
		String reducerSlaveIP;
		synchronized(slaveID2Addr)
		{
			reducerSlaveIP = slaveID2Addr.get(reducerSlaveID);
		}
		MapReduceJob curJob = curTask.getJob();
		int curJobID = curJob.getJobID();
		ArrayList<Integer> taskList;
		synchronized(jobID2TaskID)
		{
			taskList = jobID2TaskID.get(curJobID);
		}
		for (int mapTaskID : taskList)
		{
			Task prevTask;
			synchronized(taskID2Task)
			{
				prevTask = taskID2Task.get(mapTaskID);
			}
			
			// this is a map task, which belongs to this job
			if(prevTask.getPhase().equals("map"))
			{
				int slaveID;
				synchronized(taskID2SlaveID)
				{
					slaveID = taskID2SlaveID.get(prevTask.getTaskID());		// this is the slave that this map ran on.
				}
				String targetSlaveIP;
				synchronized(slaveID2Addr)
				{
					targetSlaveIP = slaveID2Addr.get(slaveID);			// this is the slave's ip.
				}
				int prevTaskID = prevTask.getTaskID();
				int reducerID = curTask.getReducerID();
				String outputFileName = "Job" + curJobID + "_Task" + prevTaskID + "_MapOutput_ForReducer" + reducerID;
				Socket socket = new Socket(reducerSlaveIP, Configuration.slavePort);
				//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Message message = new Message();
				message.withIPAddr(targetSlaveIP)
				.withOp("fetchFileFromMapper")
				.withFileName(outputFileName);
				oos.writeObject(message);
				Thread.sleep(100);
				socket.close();
			}
		}
	}
	
	// when a task is done, update information, and check whether the whole round is complete
	public void taskComplete(Task task) throws Exception
	{
		// first move this task out of runningTasks
		int taskID = task.getTaskID();
		System.out.println("entering taskComplete, taskID = " + taskID);
		synchronized(runningTasks)
		{
			runningTasks.remove(Integer.valueOf(taskID));
		}
		
		// then check whether any task of this job is still running
		int jobID = task.getJob().getJobID();
		boolean roundDone = true;
		for(Integer tempTaskID : runningTasks)
		{
			Task tempTask = taskID2Task.get(tempTaskID);
			if(tempTask.getJob().getJobID() == jobID)
			{
				roundDone = false;
				break;
			}
		}
		if(roundDone)
			taskRoundComplete(task);
	}
	
	// when a round of task is complete, decide whether to go on to reduce, or the whole job is done
	public void taskRoundComplete(Task task) throws Exception
	{
		MapReduceJob job = task.getJob();
		int jobID = job.getJobID();
		String phase = task.getPhase();
		if(phase.equals("map"))
		{
			newReduce(job);
		}
		else if(phase.equals("reduce"))
		{
			jobComplete(job);
		}
		else
		{
			System.out.println("taskRoundComplete: " + phase + "? What is that?");
		}
	}
	
	// when a job is done, update information and tell slaves to clean up temporary files
	public void jobComplete(MapReduceJob job) throws Exception
	{
		System.out.println("Job " + job.getJobID() + " with name " + job.getJobName() + " is complete!");
		synchronized(runningJobs)
		{
			runningJobs.remove(Integer.valueOf(job.getJobID()));
		}
		
		// remove all this job's tasks in slaveID2TaskID
		int completeJobID = job.getJobID();
		for(int slaveID = 0; slaveID < globalSlaveID; slaveID++)
		{
			ArrayList<Integer> taskList = slaveID2TaskID.get(slaveID);
			if(taskList == null)
				continue;
			ArrayList<Integer> deleteList = new ArrayList<Integer>();
			for(Integer curTaskID : taskList)
			{
				if(taskID2Task.get(curTaskID).getJob().getJobID() == completeJobID)
				{
					deleteList.add(curTaskID);
				}
			}
			taskList.removeAll(deleteList);
			
			String slaveIP = slaveID2Addr.get(slaveID);
			Socket socket = new Socket(slaveIP, Configuration.slavePort);
			
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			Message message = new Message();
			message.withOp("cleanUp")
			.withID(completeJobID);
			oos.writeObject(message);
			Thread.sleep(100);
			socket.close();
		}
		String outputFile = job.getOutputFile();
		for(int i = 0; i < Configuration.reduceNum; i++)
		{
			File file = new File(outputFile + "_" + i);
			file.delete();
		}
	}
	
	// distribute all tasks in the list to slaves
	public void distributeTasks(List<Task> taskList) throws Exception
	{
		for(Task curTask : taskList)
		{
			int bestSlaveID = findBestSlave(curTask);	// find the best slave to dispatch it
			
			if(curTask.getPhase().equals("reduce"))
				notifyReducerFetchFile(bestSlaveID, (ReduceTask)curTask);
			
			giveTaskToSlave(curTask, bestSlaveID);
			int taskID = curTask.getTaskID();
			
			// update slaveID2TaskID with this task
			ArrayList<Integer> taskIDList;
			synchronized(slaveID2TaskID)
			{
				taskIDList = slaveID2TaskID.get(bestSlaveID);
			}
        	if(taskIDList == null)
        	{
        		taskIDList = new ArrayList<Integer>();
        		taskIDList.add(taskID);
        		synchronized(slaveID2TaskID)
    			{
        			slaveID2TaskID.put(bestSlaveID, taskIDList);
    			}
        	}
        	else
        	{
        		taskIDList.add(taskID);
        		synchronized(slaveID2TaskID)
    			{
        			slaveID2TaskID.remove(bestSlaveID);
        			slaveID2TaskID.put(bestSlaveID, taskIDList);
    			}
        	}
        	synchronized(taskID2SlaveID)
        	{
        		taskID2SlaveID.put(taskID, bestSlaveID);
        	}
		}
		System.out.println("distributeTasks: " + taskList.size() + " tasks distributed!");
	}
	
	// find the best slave to handle this task
	public int findBestSlave(Task task)
	{
		// TODO: take file locality into consideration
		int minTasks = 100;
		int currentChoice = 0;
		int curSlaveID;
		
		// try to find the slave that has fewest running tasks
		for(curSlaveID = 0; curSlaveID < globalSlaveID; curSlaveID++)
		{
			// this slave is dead. next one.
			if(slaveID2Addr.get(curSlaveID) == null)
				continue;
			
			ArrayList<Integer> taskList;
			synchronized(slaveID2TaskID)
			{
				taskList = slaveID2TaskID.get(curSlaveID);
			}
			// if find a slave without a single task
			if(taskList == null)
			{
				return curSlaveID;
			}
			int taskNum = taskList.size();
			if(minTasks > taskNum)
			{
				minTasks = taskNum;
				currentChoice = curSlaveID;
			}
		}
		return currentChoice;
	}
	
	// dispatch task to the specified slave
	public void giveTaskToSlave(Task task, int slaveID) throws Exception
	{
		String slaveIP;
		synchronized(slaveID2Addr)
		{
			slaveIP = slaveID2Addr.get(slaveID);
		}
		int slavePort = Configuration.slavePort;
		Socket socket = new Socket(slaveIP, slavePort);
		//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		Message message = new Message();
		if(task.getPhase().equals("map"))
			message.withOp("giveMapTask");
		else if(task.getPhase().equals("reduce"))
			message.withOp("giveReduceTask");
		message.withTask(task);
		oos.writeObject(message);
		System.out.println("Task " + task.getTaskID() + " is given to slave " + slaveID);
		Thread.sleep(100);
		socket.close();
	}
	
	// find a slave for uploading files to DFS.
	// generate random numbers in order to balance the load
	public ArrayList<String> findIPAddrForUpload(String fileName, String fromIP)
	{
		Iterator iter;
		synchronized(slaveID2Addr)
		{
			iter = slaveID2Addr.entrySet().iterator(); 
		}
		
		ArrayList<String> IPList = new ArrayList<String>();
    	while (iter.hasNext())
    	{ 
    	    Map.Entry entry = (Map.Entry) iter.next();
    	    Integer slaveID = (Integer)entry.getKey(); 
    	    String slaveIP = (String)entry.getValue();
    	    // if it is the IP where the request is from, then don't send this IP back
    	    if(slaveIP.equals(fromIP))
    	    	continue;
    	    IPList.add(slaveIP);
    	}
    	int count = 0;
    	ArrayList<String> resultList = new ArrayList<String>();
    	int size = IPList.size();
    	for(String s : IPList)
    	{
    		if(count >= Configuration.REP_FACTOR - 1)
    			break;
    		Random random = new Random();
    		
    		int index;
    		while(true)
    		{
	    		index = random.nextInt(size);
	    		if(!resultList.contains(IPList.get(index)))
	    		{
		    		resultList.add(IPList.get(index));
		    		count++;
		    		break;
	    		}
    		}
    	}
    	nameNode.addFileMetadata(fileName, fromIP);
    	return resultList;
	}
	
	// recovery if a slave fails
	public void onSlaveFailure(int slaveID) throws Exception
	{
		System.out.println("slave " + slaveID + " failure notified!");
		ArrayList<Integer> taskIDList = slaveID2TaskID.get(slaveID);	// all the tasks on this slave
		
		// remove it on slaveID2Addr, so that the master knows this slave is not there.
		// from now on, scheduler will not dsitribute tasks to this slave
		synchronized(slaveID2Addr)
		{
			slaveID2Addr.remove(Integer.valueOf(slaveID));
		}
		
		// remove the record of this slave in slaveID2TaskID.
		// so that when judging whether a task round is complete,
		// tasks used to run on this slave won't interfere the decision
		synchronized(slaveID2TaskID)
		{
			slaveID2TaskID.remove(Integer.valueOf(slaveID));
		}
		
		// remove all these tasks from taskID2SlaveID.
		// because these tasks will soon find a new slave.
		// by then, there will be a new record in taskID2SlaveID
		synchronized(taskID2SlaveID)
		{
			if(taskIDList != null)
			{
				for(Integer taskID : taskIDList)
				{
					taskID2SlaveID.remove(taskID);
				}
			}
		}
		
		// re-schedule all these tasks on this dead slave
		ArrayList<Task> taskList = new ArrayList<Task>();
		for(Integer curTaskID : taskIDList)
		{
			Task curTask = taskID2Task.get(curTaskID);
			taskList.add(curTask);
		}
		distributeTasks(taskList);
	}
	
	// when user wants the whole system to abort,
	// first tell all slaves to quit, then quit.
	public void quit() throws Exception
	{
		System.out.println("Telling slaves to quit...");
		for(int slaveID = 0; slaveID < globalSlaveID; slaveID++)
		{
			String IPAddr = slaveID2Addr.get(slaveID);
			System.out.println("IPAddr: " + IPAddr);
			if(IPAddr != null)
			{
				Socket socket = new Socket(IPAddr, Configuration.slavePort);
				//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Message message = new Message();
				message.withOp("quit");
				oos.writeObject(message);
				Thread.sleep(100);
				socket.close();
			}
		}
		System.out.println("All slaves terminated. Master ready to quit. Good-bye!");
		System.exit(0);
	}
	public void listAllSlaves()
	{
		for(int slaveID = 0; slaveID < globalSlaveID; slaveID++)
		{
			String IPAddr = slaveID2Addr.get(slaveID);
			if(IPAddr != null)
			{
				System.out.println("slave #" + slaveID + ": " + IPAddr);
			}
		}
	}
	public void listAllFiles()
	{
		nameNode.listAllFiles();
	}
	public void listAllTasks()
	{
		for(Integer taskID : runningTasks)
		{
			int slaveID = taskID2SlaveID.get(taskID);
			int jobID = taskID2Task.get(taskID).getJob().getJobID();
			Task task = taskID2Task.get(taskID);
			String phase = task.getPhase();
			System.out.println("Task ID: " + taskID + " (" + phase + ")" + "\tJob ID: " + jobID + "\tSlave ID: " + slaveID);
		}
	}
	public static int b2i(byte[] b) {  
        int value = 0;  
        for (int i = 0; i < 4; i++) {  
            int shift = (4 - 1 - i) * 8;  
            value += (b[i] & 0x000000FF) << shift;  
        }  
        return value;  
    }
}
