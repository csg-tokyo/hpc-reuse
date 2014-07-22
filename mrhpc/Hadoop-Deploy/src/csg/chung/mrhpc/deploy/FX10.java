package csg.chung.mrhpc.deploy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import mpi.Info;
import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

public class FX10 {
	/* Don't change the below constants */
	public final static String DATA_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/data/";
	public final static String TMP_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/tmp/";
	public final static String HADOOP_FOLDER 			= Configure.DEPLOY_FOLDER + "/hadoop/code/";
	public final static String OPENMPI_JAVA_LIB 		= Configure.DEPLOY_FOLDER + "/openmpi/lib/";

	// Private variable
	private Intercomm spawn[];
	private List<Intercomm> mrSpawn;
	private List<Request> mrRequest;
	private List<CharBuffer> mrMessage;
	private int rank, numberSlaves;	
	
	/**
	 * Deploy Hadoop on every node
	 */
	public FX10(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			int size = MPI.COMM_WORLD.getSize();
			numberSlaves = Configure.NUMBER_OF_NODEMANAGER;
			
			spawn = new Intercomm[numberSlaves];
			mrSpawn = new ArrayList<Intercomm>();
			mrRequest = new ArrayList<Request>();
			mrMessage = new ArrayList<CharBuffer>();
			
			// Print node info
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("P" + rank + "/" + size +  ": " + ip.getHostName() + " - " + ip.getHostAddress());	
			
			// Master node
			if (rank == 0){
				initialize();
				startNameNode(rank);
			}
			
			// Datanode
			if (rank != 0 && rank <= Configure.NUMBER_OF_DATANODE){
				startDataNode(rank);
			}
			
			// Spawn NodeManager
			if (numberSlaves > 0) {
				spawnOnSlaves(0, numberSlaves);
				Request request[] = new Request[numberSlaves];
				CharBuffer message[] = new CharBuffer[numberSlaves];
				for (int i = 0; i < numberSlaves; i++){
					message[i] = ByteBuffer.allocateDirect(500).asCharBuffer();
					request[i] = spawn[i].iRecv(message[i], 500, MPI.CHAR, 0, Constants.TAG);
				}
				while (true) {
					for (int i = 0; i < numberSlaves; i++) {
						if (request[i].test()){
							String cmd = message[i].toString().trim();
							//cmd = cmd.replace("default_container_executor.sh", "launch_container.sh");
							runCommand("sed -i.bak 's/setsid //g' " + cmd);
							System.out.println("slave" + (i+1) + " start spawning Grand Child: " + cmd);
							spawnGrandChild(cmd, spawn[i], 0, "slave" + (i+1));
							message[i] = ByteBuffer.allocateDirect(500).asCharBuffer();
							request[i] = spawn[i].iRecv(message[i], 500, MPI.CHAR, 0, Constants.TAG);
						}
					}
					
					int count = 0;
					while (count < mrSpawn.size()){
						if (mrRequest.get(count).test()){
							System.out.println("Count: " + count);
							String path = mrMessage.get(count).toString().trim();
							System.out.println("Receive: " + path);
							CharBuffer mes = ByteBuffer.allocateDirect(500).asCharBuffer();
							Request req = mrSpawn.get(count).iRecv(mes, 500, MPI.CHAR, 0, Constants.TAG);
							
							mrMessage.set(count, mes);
							mrRequest.set(count, req);							
							// Read file here
							readAndSend(mrSpawn.get(count), path);
						}
						count++;
					}
				}
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

	public void spawnOnSlaves(int masterRank, int numberSlaves){
		try {
			String commands[] = new String[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				commands[i] = FX10.HADOOP_FOLDER + masterRank + "/sbin/yarn-daemon-nodemanager.sh";
				//commands[i] = "java";
			}

			String params[][] = new String[numberSlaves][];
			for (int i = 0; i < numberSlaves; i++) {
				params[i] = new String[2];
				params[i][0] = "start";
				params[i][1] = "nodemanager";				
			}

			int procs[] = new int[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				procs[i] = 1;
			}			
			
			Info infos[] = new Info[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				infos[i] = new Info();
			}

			for (int i = 0; i < numberSlaves; i++) {
				int error[] = new int[procs[i]];
				System.out.println("Node " + rank + " is spawning NodeManager.");
				spawn[i] = MPI.COMM_WORLD.spawn(commands[i], params[i], procs[i], infos[i], 0, error);
				if (rank == 0) {
					for (int j = 0; j < procs[i]; j++) {
						if (error[j] == MPI.SUCCESS) {
							System.out.println("Spawn " + i + " OK");
						}
					}
				}				
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	public void spawnGrandChild(String cmd, Intercomm group, int child, String host){
		try {
			String params[] = {};
			int proc = 1;
			Info info = new Info();
			info.set("host", host);
			int error[] = new int[proc];
			Intercomm spawnChild = MPI.COMM_WORLD.spawn(cmd, params, proc, info, 0, error);
			if (error[0] == MPI.SUCCESS) {
				System.out.println("Grand child " + host + " Spawned " + " OK");
				CharBuffer message = ByteBuffer.allocateDirect(500).asCharBuffer();
				Request request = spawnChild.iRecv(message, 500, MPI.CHAR, 0, Constants.TAG);
				mrMessage.add(message);
				mrRequest.add(request);
				mrSpawn.add(spawnChild);
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}			
	
	public void readAndSend(Intercomm group, String path){
		try {
			File file = new File(path);
			byte[] bytes = new byte[(int) file.length()];
			FileInputStream fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytes);
			fileInputStream.close();	
			System.out.println("Sending back... --> Length: " + bytes.length);
			int length[] = new int[1];
			length[0] = bytes.length;
			group.send(length, 1, MPI.INT, 0, Constants.TAG);
			group.send(bytes, bytes.length, MPI.BYTE, 0, Constants.TAG);
			System.out.println("Sending OK.");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MPIException e) {
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
	public void startNameNode(int rank){
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			generateCode(rank, ip.getHostAddress());
			
			// Start master
			File folder = new File(FX10.DATA_FOLDER + rank);
			if (folder.isDirectory() && folder.list().length == 0){
				runCommand(FX10.HADOOP_FOLDER + rank + "/bin/hdfs namenode -format");
				System.out.println("Format Namenode --> OK");
			}
			
			runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/hadoop-daemon.sh start namenode");
			System.out.println("Start NameNode --> OK");
			runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/yarn-daemon.sh start resourcemanager");
			System.out.println("Start Resource Manager --> OK");
			
			// Send master address to DataNode
			 char[] message = ip.getHostAddress().toCharArray();
			 for (int i=1; i <= Configure.NUMBER_OF_DATANODE; i++){
				 MPI.COMM_WORLD.send(message, message.length, MPI.CHAR, i, 99);	
			 }	 
			 System.out.println("NameNode sending its IP address --> OK");
		} catch (MPIException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
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
			if (rank <= Configure.NUMBER_OF_DATANODE){
				runCommand(FX10.HADOOP_FOLDER + rank + "/sbin/hadoop-daemon.sh start datanode");
			}
			System.out.println("Start DataNode " + rank + " --> OK");
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
			File folder = new File(FX10.DATA_FOLDER + rank);
			if (!folder.exists()){
				runCommand("mkdir " + DATA_FOLDER + rank);
			}
			
			runCommand("tar -zxf " + Configure.HADOOP_TAR_GZ_FILE + " --directory=" + FX10.HADOOP_FOLDER + " --transform s/hadoop/" + rank + "/");
			String HADOOP_INSTALL = FX10.HADOOP_FOLDER + rank;
			
			// Hadoop environment
			runCommand("sed -i.bak 's/MRHPC_JAVA_HOME/" + Configure.JAVA_HOME.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");
			String HADOOP_CONF_DIR = HADOOP_INSTALL + "/etc/hadoop";
			runCommand("sed -i.bak 's/MRHPC_HADOOP_CONF_DIR/" + HADOOP_CONF_DIR.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");						
			runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hadoop-env.sh");
			
			// CORE
			runCommand("sed -i.bak 's/MRHPC_MASTER/" + masterAddress + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			runCommand("sed -i.bak 's/MRHPC_USERNAME/" + Configure.USERNAME + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			runCommand("sed -i.bak 's/MRHPC_TMP_FOLDER/" + TMP_FOLDER.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/core-site.xml");
			
			// HDFS
			runCommand("sed -i.bak 's/MRHPC_DATA_FOLDER/" + DATA_FOLDER.replaceAll("/", "\\\\/") + rank + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hdfs-site.xml");
			runCommand("sed -i.bak 's/MRHPC_USERNAME/" + Configure.USERNAME + "/g' " + HADOOP_INSTALL + "/etc/hadoop/hdfs-site.xml");
		
			// YARN
			runCommand("sed -i.bak 's/MRHPC_JAVA_HOME/" + Configure.JAVA_HOME.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-env.sh");
			String mpiJar = Configure.DEPLOY_FOLDER + "/openmpi/lib/mpi.jar";
			runCommand("sed -i.bak 's/MRHPC_OPENMPI_MPI_JAR/" + mpiJar.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-env.sh");			
			runCommand("sed -i.bak 's/MRHPC_MASTER/" + masterAddress + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");
			runCommand("sed -i.bak 's/MRHPC_HADOOP_CONF_DIR/" + HADOOP_CONF_DIR.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");						
			runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");
			runCommand("sed -i.bak 's/MRHPC_OPENMPI_MPI_JAR/" + mpiJar.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/yarn-site.xml");						
			
			// MAP-REDUCE
			runCommand("sed -i.bak 's/MRHPC_OPENMPI_JAVA_LIB/" + OPENMPI_JAVA_LIB.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/mapred-site.xml");
			runCommand("sed -i.bak 's/MRHPC_HADOOP_INSTALL/" + HADOOP_INSTALL.replaceAll("/", "\\\\/") + "/g' " + HADOOP_INSTALL + "/etc/hadoop/mapred-site.xml");			
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
