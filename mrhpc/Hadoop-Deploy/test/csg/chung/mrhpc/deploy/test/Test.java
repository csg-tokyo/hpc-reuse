package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.MPI;
import mpi.MPIException;

public class Test {
	
	public Test() throws MPIException, UnknownHostException{			
			int rank = MPI.COMM_WORLD.getRank();
			if (rank == 0){
				int length[] = new int[1];
				length[0] = 30;
				MPI.COMM_WORLD.send(length, 1, MPI.INT, 1, 99);

				InetAddress ip=InetAddress.getLocalHost();			
				char[] message = ip.getHostAddress().toCharArray();		
				MPI.COMM_WORLD.send(message, message.length, MPI.CHAR, 1, 99);					
			}
			if (rank == 1){
				int length[] = new int[1];
				System.out.println("Start receiving");
				MPI.COMM_WORLD.recv(length, 1, MPI.INT, 0, 99);
				System.out.println("Receive " + length[0]);
				
				char[] message = new char[20]; 
				int master = 0;
				MPI.COMM_WORLD.recv(message, 20, MPI.CHAR, master, 99);	
				String masterAddress = String.valueOf(message).trim();	
				System.out.println("Master address: " + masterAddress);				
			}
	}
	
	public static void main(String args[]){
		//MPI.Init(args);
		System.out.println(System.getProperty("java.class.path"));
		//MPI.Finalize();
	}
}
