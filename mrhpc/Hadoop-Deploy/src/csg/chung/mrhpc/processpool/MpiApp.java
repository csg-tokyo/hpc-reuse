package csg.chung.mrhpc.processpool;

import mpi.MPI;
import mpi.MPIException;
import csg.chung.mrhpc.utils.Lib;

public class MpiApp {
	/* Don't change the below constants */
	public final static String DATA_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/data/";
	public final static String TMP_FOLDER 				= Configure.DEPLOY_FOLDER + "/hadoop/tmp/";
	public final static String HADOOP_FOLDER 			= Configure.DEPLOY_FOLDER + "/hadoop/code/";
	public final static String OPENMPI_JAVA_LIB 		= Configure.DEPLOY_FOLDER + "/openmpi/lib/";	
	
	private int rank, size;
	
	public MpiApp(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize() - Configure.NUMBER_PROCESS_EACH_NODE;
			
			if (rank >= size){
			}else{
				// Print node info
				Lib.printNodeInfo(rank, size);
						
				startNonMPIProcess();
				startMPIProcess();
			}
		}catch(MPIException e){
			e.printStackTrace();
		}
	}	
	
	public void startMPIProcess(){
		// Start every process
		if (rank / Configure.NUMBER_PROCESS_EACH_NODE > 0){
			if (rank % Configure.NUMBER_PROCESS_EACH_NODE == 0){
				Pool p = new Pool(rank);
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
			}
		}
	}	
	
	/**
	 * Initialize installation
	 */
	public void initialize(){
		Lib.runCommand("mkdir " + Configure.DEPLOY_FOLDER + "/hadoop");
		Lib.runCommand("mkdir " + DATA_FOLDER);
		Lib.runCommand("mkdir " + TMP_FOLDER);
		Lib.runCommand("mkdir " + HADOOP_FOLDER);
		Lib.runCommand("mkdir " + Configure.LOCK_FILE_PATH);
	}
	
	public static void main(String[] args) throws MPIException {
		MPI.InitThread(args, MPI.THREAD_SERIALIZED);
		new MpiApp();
		
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
