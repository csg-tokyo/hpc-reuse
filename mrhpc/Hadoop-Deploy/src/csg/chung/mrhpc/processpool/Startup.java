package csg.chung.mrhpc.processpool;

import csg.chung.mrhpc.utils.Lib;
import mpi.MPI;
import mpi.MPIException;

public class Startup {
	public final static String HADOOP_FOLDER 			= "/home/mrhpc/hadoop";
	private int rank, size;
	
	public Startup(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize();
			
			// Print node info
			Lib.printNodeInfo(rank, size);
			
			startNonMPIProcess();
			startMPIProcess();
			
		}catch(MPIException e){
			e.printStackTrace();
		}
	}
	
	public void startMPIProcess(){
		// Start every process
		if (rank % Configure.NUMBER_PROCESS_EACH_NODE == 0){
			Pool p = new Pool(rank);
			if (rank > 0){
				String prop = "-Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir= -Dyarn.id.str=mrhpc -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Dyarn.policy.file=hadoop-policy.xml -server -Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir=/home/mrhpc/hadoop -Dhadoop.home.dir=/home/mrhpc/hadoop -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA";
				String className = "org.apache.hadoop.yarn.server.nodemanager.NodeManager";	
				p.startNewProcess(prop, className, "");
				p.waiting();
			}			
		}else{
			new Process(rank).waiting();
		}					
	}
	
	public void startNonMPIProcess(){
		if (rank % Configure.NUMBER_PROCESS_EACH_NODE == 0){
			if (rank == 0){
				startMaster(rank);
			}else{
				startSlaves(rank);
			}
		}
	}
		
	public void startMaster(int rank){
		Lib.runCommand(HADOOP_FOLDER + "/sbin/hadoop-daemon.sh start namenode");
		Lib.runCommand(HADOOP_FOLDER + "/sbin/yarn-daemon.sh start resourcemanager");
		System.out.println("Start Master " + rank + " --> OK");		
	}
	
	public void startSlaves(int rank){
		Lib.runCommand(HADOOP_FOLDER + "/sbin/hadoop-daemon.sh start datanode");
		System.out.println("Start Slave " + rank + " --> OK");	
	}		
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new Startup();
		MPI.Finalize();
	}	
	
}
