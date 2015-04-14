package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.MPI;
import mpi.MPIException;

public class SpawnChild {
	private int rank, size;
	
	public SpawnChild() throws MPIException, UnknownHostException{
		rank = MPI.COMM_WORLD.getRank();
		size = MPI.COMM_WORLD.getSize();
		
		InetAddress ip = InetAddress.getLocalHost();
		System.out.println("Child " + rank + "/" + size + ": " + ip.getHostName()
				+ " - " + ip.getHostAddress());		
	}
	
	public static void main(String args[]) throws MPIException, UnknownHostException{
		MPI.Init(args);
		new SpawnChild();
		MPI.Finalize();
	}	
}
