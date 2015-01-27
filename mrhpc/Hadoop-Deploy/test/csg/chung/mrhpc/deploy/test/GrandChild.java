package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.MPI;
import mpi.MPIException;

public class GrandChild {
	private int rank;
	private String host;

	public GrandChild(String args[]){
		try {
			MPI.Init(args);
			rank = MPI.COMM_WORLD.getRank();

			InetAddress ip = InetAddress.getLocalHost();
			host = ip.getHostName();
			System.out.println("Grand Child " + rank + ": " + host
					+ " - " + ip.getHostAddress());
			
			MPI.Finalize();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static void main(String args[]) {
		new GrandChild(args);
	}
}
