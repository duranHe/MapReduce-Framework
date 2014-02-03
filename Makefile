JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	Configuration/Configuration.java \
	DFS/NameNode.java \
	DFS/DataNode.java \
	Example/WordCount.java \
	Example/LargestWord.java \
	Master/Master.java \
	Master/MasterCommandThread.java \
	Master/MasterCommThread.java \
	Master/MasterManager.java \
	Slave/Slave.java \
	Slave/SlaveCommandThread.java \
	Slave/SlaveCommThread.java \
	Slave/SlaveManager.java \
	Util/FilePartition.java \
	Util/MapReduceJob.java \
	Util/MapTask.java \
	Util/MapTaskThread.java \
	Util/Message.java \
	Util/RecordReader.java \
	Util/RecordWriter.java \
	Util/ReduceTask.java \
	Util/ReduceTaskThread.java \
	Util/Task.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) Configuration/*.class
	$(RM) DFS/*.class
	$(RM) Example/*.class
	$(RM) Master/*.class
	$(RM) Slave/*.class
	$(RM) Util/*.class
