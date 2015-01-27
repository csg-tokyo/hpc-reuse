package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import csg.chung.mrhpc.deploy.Constants;

import mpi.Info;
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
			
			//receiveFromParent(parent, 0);
			//spawnGrandChild();
			System.out.println("Parent size: " + parent.getRemoteSize());
			for (int i=0; i < parent.getRemoteSize(); i++){
				sendSpawnToParent(parent, i);
			}
			
			MPI.Finalize();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void spawnGrandChild(){
		try {
			String cmd = "Child.sh";
			String params[] = {};
			int proc = 1;
			Info info = new Info();
			InetAddress ip = InetAddress.getLocalHost();
			info.set("host", ip.getHostName());
			int error[] = new int[proc];
			MPI.COMM_WORLD.spawn(cmd, params, proc, info, 0, error);
			if (error[0] == MPI.SUCCESS) {
				System.out.println("Child at " + host + " Spawned " + " OK");
			}
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

	public void sendSpawnToParent(Intercomm group, int parent){
		try {
			String cmd = "Child.sh";
			char[] message = cmd.toCharArray();			
			group.send(message, message.length, MPI.CHAR, parent, Constants.TAG);
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static void main(String args[]) {
		new Child(args);
	}
}
