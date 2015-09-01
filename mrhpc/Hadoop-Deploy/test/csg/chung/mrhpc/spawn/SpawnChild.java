package csg.chung.mrhpc.spawn;

import java.net.UnknownHostException;

import csg.chung.mrhpc.utils.Lib;

import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;

public class SpawnChild {
	private int rank, size;
	
	public SpawnChild() throws MPIException, UnknownHostException{
		rank = MPI.COMM_WORLD.getRank();
		size = MPI.COMM_WORLD.getSize();
		Intercomm parent = Intercomm.getParent();
		parent.merge(true);
		//Lib.printNodeInfo(rank, size, "Child:");
	}
	
	public static void main(String args[]) throws MPIException, UnknownHostException{
		MPI.Init(args);
		new SpawnChild();
		MPI.Finalize();
	}	
}
