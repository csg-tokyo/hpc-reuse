package csg.chung.mrhpc.deploy.test;
import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;


public class Child {
	
	public static void main(String args[]){
		try {
			MPI.Init(args);
			Intercomm parent;
			parent = Intercomm.getParent();
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("Child: parent has " + parent.getSize() + ": " + ip.getHostName() + " - " + ip.getHostAddress());			
			MPI.Finalize();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
