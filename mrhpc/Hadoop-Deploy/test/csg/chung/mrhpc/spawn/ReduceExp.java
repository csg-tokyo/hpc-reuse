package csg.chung.mrhpc.spawn;

import mpi.MPI;
import mpi.MPIException;

public class ReduceExp {
	public ReduceExp() throws MPIException{
		int rank = MPI.COMM_WORLD.getRank();
		
		int[] send = new int[1];
		send[0] = rank * 2;
		int[] receive = new int[1];
		receive[0] = 0;
		
		if (rank == 0){
			System.out.println("Recv: " + receive[0]);
		}		
		
		MPI.COMM_WORLD.reduce(send, receive, 1, MPI.INT, MPI.MAX, 0);
		
		if (rank == 0){
			System.out.println("Recv: " + receive[0]);
		}
	}
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new ReduceExp();
		MPI.Finalize();
	}
}
