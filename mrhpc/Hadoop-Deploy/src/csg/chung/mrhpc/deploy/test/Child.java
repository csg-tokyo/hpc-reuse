package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import csg.chung.mrhpc.deploy.Constants;

import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;

public class Child {

	private int rank;
	private String host;
	
	public Child(String args[]) {
		try {
			MPI.Init(args);
			Intercomm parent = Intercomm.getParent();
			rank = MPI.COMM_WORLD.getRank();

			InetAddress ip = InetAddress.getLocalHost();
			host = ip.getHostName();
			System.out.println("Child " + rank + ": " + ip.getHostName()
					+ " - " + ip.getHostAddress());
			
			receiveFromParent(parent, 0);
			
			MPI.Finalize();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void receiveFromParent(Intercomm group, int parent){
		try {
			char[] message = new char[20]; 
			group.recv(message, 20, MPI.CHAR, parent, Constants.TAG);
			String recv = String.valueOf(message).trim();
			System.out.println("Child " + rank + ": " + host + " - " + recv);			
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	public static void main(String args[]) {
		new Child(args);
	}
}
