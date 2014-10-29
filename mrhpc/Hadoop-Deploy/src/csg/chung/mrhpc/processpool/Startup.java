package csg.chung.mrhpc.processpool;

import csg.chung.mrhpc.utils.Lib;
import mpi.MPI;
import mpi.MPIException;

public class Startup {
	public final static int NUMBER_PROCESS_EACH_NODE = 5;
	private int rank, size;
	
	public Startup(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize();
			
			// Print node info
			Lib.printNodeInfo(rank, size);
			
			// Start every process
			if (rank % NUMBER_PROCESS_EACH_NODE == 0){
				new Pool(rank);
			}else{
				new Process().waiting();
			}			
		}catch(MPIException e){
			e.printStackTrace();
		}
	}
			
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new Startup();
		MPI.Finalize();
	}	
	
}
