package csg.chung.mrhpc.processpool;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import mpi.MPI;
import mpi.MPIException;
import csg.chung.mrhpc.utils.Lib;

public class FX10 {
	/* Don't change the below constants */
	public final static String DATA_FOLDER 				= Configure.DATA_FOLDER + "/";
	public final static String TMP_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/tmp/";
	public final static String HADOOP_FOLDER 			= Configure.DEPLOY_FOLDER + "/hadoop/code/";
	public final static String OPENMPI_JAVA_LIB 		= Configure.DEPLOY_FOLDER + "/openmpi/lib/";	
	
	private int rank, size;
	
	public FX10(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize() - Configure.NUMBER_PROCESS_EACH_NODE;
			
			if (rank >= size){
				if (rank == size){
					 // Run MapReduce job
					 Thread.sleep(120*1000);
					 Lib.runCommand(Configure.MAPREDUCE_JOB);
					 System.out.println("Running MapReduce jobs");					
				}
			}else{
				// Print node info
				Lib.printNodeInfo(rank, size);
						
				startNonMPIProcess();
				startMPIProcess();
			}
		}catch(MPIException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	public void startMPIProcess(){
		// Start every process
		if (rank / Configure.NUMBER_PROCESS_EACH_NODE > 0){
			if (rank % Configure.NUMBER_PROCESS_EACH_NODE == 0){
				Pool p = new Pool(rank);
				String hadoopFolder = HADOOP_FOLDER + rank; 
				String logFolder = hadoopFolder + "/logs"; 				
				String prop = 	"-Dhostname=" + Lib.getHostname() + " -Dhadoop.log.dir=" + logFolder + " -Dyarn.log.dir=" + logFolder + 
								" -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave" + rank + ".log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave" + rank + ".log -Dyarn.home.dir= -Dyarn.id.str=mrhpc -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Dyarn.policy.file=hadoop-policy.xml -server -Dyarn.home.dir=" + hadoopFolder + 
								" -Dhadoop.home.dir=" + hadoopFolder + " -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA";
				String className = "org.apache.hadoop.yarn.server.nodemanager.NodeManager";	
				p.startNewProcess(prop, className, "");
				p.waiting();
				
			}else{
				new Process(rank).waiting();			
			}
		}					
	}
	
	public void startNonMPIProcess(){		
		if (rank % Configure.NUMBER_PROCESS_EACH_NODE == 0){
			Lib.runCommand("mkdir " + Configure.ANALYSIS_LOG);			
			Lib.runCommand("java csg.chung.mrhpc.deploy.test.CPUUsage &> " + Configure.CPU_LOG + rank + ".txt &");
			if (rank == 0){
				initialize();
				startNameNode(rank);
			}else{
				startDataNode(rank);
			}
		}
	}	
	
	/**
	 * Initialize installation
	 */
	public void initialize(){
		Lib.runCommand("mkdir " + Configure.DEPLOY_FOLDER + "/hadoop");
		Lib.runCommand("mkdir " + TMP_FOLDER);
		Lib.runCommand("mkdir " + HADOOP_FOLDER);
		Lib.runCommand("mkdir " + Configure.LOCK_FILE_PATH);
	}
	
	/**
	 * Start Namenode and Resource manager
	 * @param rank Node ID that is responsible for
	 */
	public void startNameNode(int rank){
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			generateCode(rank, ip.getHostAddress());
			
			// Start master
			File folder = new File(FX10.DATA_FOLDER + (rank/Configure.NUMBER_PROCESS_EACH_NODE));
			if (folder.isDirectory() && folder.list().length == 0){
				Lib.runCommand(FX10.HADOOP_FOLDER + rank + "/bin/hdfs namenode -format");
				System.out.println("Format Namenode --> OK");
			}
			
			Lib.runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/hadoop-daemon.sh start namenode");
			System.out.println("Start NameNode --> OK");
			Lib.runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/yarn-daemon.sh start resourcemanager");
			System.out.println("Start Resource Manager --> OK");
			
			// Send master address to DataNode
			 char[] message = ip.getHostAddress().toCharArray();
			 for (int i= Configure.NUMBER_PROCESS_EACH_NODE; i < size; i = i + Configure.NUMBER_PROCESS_EACH_NODE){
				 MPI.COMM_WORLD.send(message, message.length, MPI.CHAR, i, 99);	
			 }	 
			 System.out.println("NameNode sending its IP address --> OK");
		} catch (MPIException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Start DataNode
	 * @param rank Node ID that is responsible for
	 */
	public void startDataNode(int rank){
		try {
			// Wait for NameNode
			System.out.println("DataNode " + rank + " is waiting");
			char[] message = new char[20]; 
			int master = 0;
			MPI.COMM_WORLD.recv(message, 20, MPI.CHAR, master, 99);
			
			String masterAddress = String.valueOf(message).trim();
			generateCode(rank, masterAddress);
			System.out.println("DataNode " + rank + " is starting");
			
			// Start DataNode
			Lib.runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/hadoop-daemon.sh start datanode");
			//Lib.runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/yarn-daemon.sh start nodemanager");
			
			System.out.println("Start DataNode " + rank + " --> OK");
			//Lib.runCommand("java csg.chung.mrhpc.deploy.test.SortClient >> /mppxb/c83014/tuning/rank/" + rank + ".txt 2>&1");
			//new SortThread(rank).start();
			//new SortClient(rank);
		} catch (MPIException e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * Replace the Hadoop source code for each corresponding type of node
	 * @param rank
	 * @param masterAddress
	 */
	public void generateCode(int rank, String masterAddress){
		File dataFolder = new File(FX10.DATA_FOLDER + (rank/Configure.NUMBER_PROCESS_EACH_NODE));
		if (!dataFolder.exists()){
			Lib.runCommand("mkdir " + DATA_FOLDER + (rank/Configure.NUMBER_PROCESS_EACH_NODE));
		}
		
		File tmpFolder = new File(FX10.TMP_FOLDER + Lib.getHostname());
		if (!tmpFolder.exists()){
			Lib.runCommand("mkdir " + FX10.TMP_FOLDER + Lib.getHostname());
		}		
			
		Lib.runCommand("tar -zxf " + Configure.HADOOP_TAR_GZ_FILE + " --directory=" + FX10.HADOOP_FOLDER + " --transform s/hadoop/" + rank + "/");
		String HADOOP_INSTALL = FX10.HADOOP_FOLDER + rank;
			
		// Hadoop environment
		Lib.runCommand("sed -i.bak 's/MRHPC_JAVA_HOME/" + Configure.JAVA_HOME.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");
		String HADOOP_CONF_DIR = HADOOP_INSTALL + "/etc/hadoop";
		Lib.runCommand("sed -i.bak 's/MRHPC_HADOOP_CONF_DIR/" + HADOOP_CONF_DIR.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");						
		Lib.runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");
			
		// CORE
		Lib.runCommand("sed -i.bak 's/MRHPC_MASTER/" + masterAddress + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
		Lib.runCommand("sed -i.bak 's/MRHPC_USERNAME/" + Configure.USERNAME + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
		Lib.runCommand("sed -i.bak 's/MRHPC_TMP_FOLDER/" + (TMP_FOLDER + "${hostname}" ).replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			
		// HDFS
		Lib.runCommand("sed -i.bak 's/MRHPC_DATA_FOLDER/" + DATA_FOLDER.replaceAll("/", "\\\\/") + (rank/Configure.NUMBER_PROCESS_EACH_NODE) + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hdfs-site.xml");
		Lib.runCommand("sed -i.bak 's/MRHPC_USERNAME/" + Configure.USERNAME + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hdfs-site.xml");
		
		// YARN
		Lib.runCommand("sed -i.bak 's/MRHPC_JAVA_HOME/" + Configure.JAVA_HOME.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-env.sh");
		String mpiJar = Configure.DEPLOY_FOLDER + "/openmpi/lib/mpi.jar";
		Lib.runCommand("sed -i.bak 's/MRHPC_OPENMPI_MPI_JAR/" + mpiJar.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-env.sh");			
		String jnaJar = Configure.DEPLOY_FOLDER + "/jna.jar";
		Lib.runCommand("sed -i.bak 's/MRHPC_JNA_JAR/" + jnaJar.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-env.sh");					
		Lib.runCommand("sed -i.bak 's/MRHPC_MASTER/" + masterAddress + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");
		Lib.runCommand("sed -i.bak 's/MRHPC_HADOOP_CONF_DIR/" + HADOOP_CONF_DIR.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");						
		Lib.runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");
		Lib.runCommand("sed -i.bak 's/MRHPC_OPENMPI_MPI_JAR/" + mpiJar.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");						
			
		// MAP-REDUCE
		Lib.runCommand("sed -i.bak 's/MRHPC_OPENMPI_JAVA_LIB/" + OPENMPI_JAVA_LIB.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/mapred-site.xml");
		Lib.runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/mapred-site.xml");			
	}		
	
	public static void main(String[] args) throws MPIException {
		MPI.InitThread(args, MPI.THREAD_SERIALIZED);
		new FX10();
		
		if (MPI.COMM_WORLD.getRank() == 0) {
			System.out.println("Hadoop is READY!!!");
			try {
				String time[] = Configure.ELAPSED_TIME.split(":");
				long elapse = Long.parseLong(time[0])*60*60*1000 + Long.parseLong(time[1])*60*1000 + Long.parseLong(time[2])*1000;
				Thread.sleep(elapse);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		MPI.Finalize();
	}	
}
