package csg.chung.mrhpc.spawn;

import csg.chung.mrhpc.utils.Lib;
import mpi.Info;
import mpi.Intercomm;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

public class SpawnParent {
	private int rank, size;
	
	public SpawnParent(int nproc) throws MPIException{
		rank = MPI.COMM_WORLD.getRank();
		size = MPI.COMM_WORLD.getSize();
		if (rank == 0){
			Lib.printNodeInfo(rank, size, "Parent:");
		}
		spawn(nproc);
	}
	
	public void spawn(int nproc) throws MPIException{
		String params[] = {"csg.chung.mrhpc.spawn.SpawnChild"};
		Info info = new Info();
		long start = 0;
		
		if (rank == 0){
			System.out.println("Rank: " + rank + " starts spawning");
			start = System.currentTimeMillis();
		}
		
		Intercomm child = MPI.COMM_WORLD.spawn("java", params, nproc, info, 0, null);
		Intracomm intra = child.merge(false);
		if (rank == 0){
			System.out.println("Spawning time of size " + nproc + ": " + (System.currentTimeMillis() - start));
		}
	}
		
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new SpawnParent(Integer.parseInt(args[0]));
		MPI.Finalize();
	}
}
