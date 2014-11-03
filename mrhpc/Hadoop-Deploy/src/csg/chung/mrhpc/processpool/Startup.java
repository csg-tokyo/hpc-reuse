package csg.chung.mrhpc.processpool;

import csg.chung.mrhpc.utils.Lib;
import mpi.MPI;
import mpi.MPIException;

public class Startup {
	public final static int NUMBER_PROCESS_EACH_NODE = 5;
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
		if (rank % NUMBER_PROCESS_EACH_NODE == 0){
			new Pool(rank);
		}else{
			new Process(rank).waiting();
		}					
	}
	
	public void startNonMPIProcess(){
		if (rank % NUMBER_PROCESS_EACH_NODE == 0){
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
