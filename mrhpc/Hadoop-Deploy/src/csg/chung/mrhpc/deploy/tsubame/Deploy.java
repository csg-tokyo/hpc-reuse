package csg.chung.mrhpc.deploy.tsubame;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;

import csg.chung.mrhpc.utils.Lib;

import mpi.MPI;
import mpi.MPIException;

public class Deploy {
	/* Don't change the below constants */
	public final static String HADOOP_TAR_GZ_FILE 		= Configure.DEPLOY_FOLDER + "/hadoop.tar.gz";
	public final static String DATA_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/data/";
	public final static String TMP_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/tmp/";
	public final static String HADOOP_FOLDER 			= Configure.DEPLOY_FOLDER + "/hadoop/code/";
	
	/**
	 * Deploy Hadoop on every node
	 */
	public Deploy(){
		try {
			int rank = MPI.COMM_WORLD.getRank();
			
			// Print node info
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("P" + rank + ": " + ip.getHostName() + " - " + ip.getHostAddress());	
			
			if (rank == 0){
				// Master node
				initialize();
				startMaster(rank);
				
				// Start job
				Thread.sleep(60*1000);
				Lib.runCommand(Configure.MAPREDUCE_JOB);
			}else{
				startSlaves(rank);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (MPIException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Initialize installation
	 */
	public void initialize(){
		try {
			runCommand("mkdir " + Configure.DEPLOY_FOLDER + "/hadoop");
			runCommand("mkdir " + DATA_FOLDER);
			runCommand("mkdir " + TMP_FOLDER);
			runCommand("mkdir " + HADOOP_FOLDER);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Start Namenode and Resource manager
	 * @param rank Node ID that is responsible for
	 */
	public void startMaster(int rank){
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			generateCode(rank, ip.getHostAddress());
			
			// Start master
			File folder = new File(Deploy.DATA_FOLDER + rank);
			if (folder.isDirectory() && folder.list().length == 0){
				runCommand(Deploy.HADOOP_FOLDER + rank + "/bin/hdfs namenode -format");
				System.out.println("Format Namenode --> OK");
			}
			
			runCommand(Deploy.HADOOP_FOLDER + rank + "/sbin/hadoop-daemon.sh start namenode");
			runCommand(Deploy.HADOOP_FOLDER + rank + "/sbin/yarn-daemon.sh start resourcemanager");
			runCommand(Deploy.HADOOP_FOLDER + rank + "/sbin/yarn-daemon.sh start nodemanager");		
			System.out.println("Start Master --> OK");
			
			// Send master address to slave nodes
			 char[] message = ip.getHostAddress().toCharArray();
			 int size = MPI.COMM_WORLD.getSize();
			 for (int i=1; i < size; i++){
				 MPI.COMM_WORLD.send(message, message.length, MPI.CHAR, i, 99);	
			 }	 
		} catch (MPIException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Start Datanode and Node manager
	 * @param rank Node ID that is responsible for
	 */
	public void startSlaves(int rank){
		try {
			// Wait for master node
			System.out.println("Slave node " + rank + " is waiting");
			char[] message = new char[20]; 
			int master = 0;
			MPI.COMM_WORLD.recv(message, 20, MPI.CHAR, master, 99);
			
			String masterAddress = String.valueOf(message).trim();
			generateCode(rank, masterAddress);
			System.out.println("Slave node " + rank + " is starting");
			
			// Start slave nodes
			runCommand(Deploy.HADOOP_FOLDER + rank + "/sbin/hadoop-daemon.sh start datanode");
			runCommand(Deploy.HADOOP_FOLDER + rank + "/sbin/yarn-daemon.sh start nodemanager");
			System.out.println("Start slave node " + rank + " --> OK");
		} catch (MPIException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Replace the Hadoop source code for each corresponding type of node
	 * @param rank
	 * @param masterAddress
	 */
	public void generateCode(int rank, String masterAddress){
		try {
			File folder = new File(Deploy.DATA_FOLDER + rank);
			if (!folder.exists()){
				runCommand("mkdir " + DATA_FOLDER + rank);
				runCommand("mkdir " + TMP_FOLDER + rank);
			}
			
			runCommand("tar -zxf " + HADOOP_TAR_GZ_FILE + " --directory=" + Deploy.HADOOP_FOLDER + " --transform s/hadoop/" + rank + "/");
			String HADOOP_INSTALL = Deploy.HADOOP_FOLDER + rank;
			
			runCommand("sed -i.bak 's/MRHPC_JAVA_HOME/" + Configure.JAVA_HOME.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");
			String HADOOP_CONF_DIR = HADOOP_INSTALL + "/etc/hadoop";
			runCommand("sed -i.bak 's/MRHPC_HADOOP_CONF_DIR/" + HADOOP_CONF_DIR.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");						
			runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");
			
			runCommand("sed -i.bak 's/MRHPC_MASTER/" + masterAddress + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			runCommand("sed -i.bak 's/MRHPC_USERNAME/" + Configure.USERNAME + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			runCommand("sed -i.bak 's/MRHPC_TMP_FOLDER/" + TMP_FOLDER.replaceAll("/", "\\\\/") + rank + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			
			runCommand("sed -i.bak 's/MRHPC_DATA_FOLDER/" + DATA_FOLDER.replaceAll("/", "\\\\/") + rank + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hdfs-site.xml");
			runCommand("sed -i.bak 's/MRHPC_USERNAME/" + Configure.USERNAME + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hdfs-site.xml");
		
			runCommand("sed -i.bak 's/MRHPC_JAVA_HOME/" + Configure.JAVA_HOME.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-env.sh");
			runCommand("sed -i.bak 's/MRHPC_MASTER/" + masterAddress + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Call bash command
	 * @param command
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void runCommand(String command)
			throws IOException, InterruptedException {		
		ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
		Process process = processBuilder.start();

		InputStream stderr = process.getErrorStream();
		InputStreamReader isr = new InputStreamReader(stderr);
		BufferedReader br = new BufferedReader(isr);
		String line;
		while ((line = br.readLine()) != null){
			System.out.println(line);
		}
		process.waitFor();
	}		
	
	public static void main(String[] args) throws MPIException {
		MPI.Init(args);
		new Deploy();
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
