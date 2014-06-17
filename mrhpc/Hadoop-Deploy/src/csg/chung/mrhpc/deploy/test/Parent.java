package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import csg.chung.mrhpc.deploy.Constants;

import mpi.Intercomm;
import mpi.MPI;
import mpi.Info;
import mpi.MPIException;

public class Parent {
	private Intercomm spawn[];
	private int rank, size;
	private int numberSlaves;

	public Parent(String args[]) {
		try {
			MPI.Init(args);
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize();
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("Parent " + rank + ": " + ip.getHostName()
					+ " - " + ip.getHostAddress());

			numberSlaves = size - 1;
			spawn = new Intercomm[numberSlaves];

			if (numberSlaves > 0) {
				spawnOnSlaves();
				if (rank == 0){
					for (int i = 0; i < numberSlaves; i++){ 	
						helloToChild(spawn[i], 0);
					}
				}
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

	public void helloToChild(Intercomm group, int child){
		try {
			String hello = "Hello from parent " + rank;
			char[] message = hello.toCharArray();			
			group.send(message, message.length, MPI.CHAR, child, Constants.TAG);
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void spawnOnSlaves() {
		try {
			String commands[] = new String[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				commands[i] = "java";
			}

			String params[][] = new String[numberSlaves][];
			for (int i = 0; i < numberSlaves; i++) {
				params[i] = new String[1];
				params[i][0] = "csg.chung.mrhpc.deploy.test.Child";
			}

			Info infos[] = new Info[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				infos[i] = new Info();
				infos[i].set("host", "slave" + (i + 1));
			}

			int procs[] = new int[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				procs[i] = 1;
			}

			// Spawn each command separately
			for (int i = 0; i < numberSlaves; i++) {
				int error[] = new int[procs[i]];
				spawn[i] = MPI.COMM_WORLD.spawn(commands[i], params[i],
						procs[i], infos[i], 0, error);
				if (rank == 0) {
					for (int j = 0; j < procs[i]; j++) {
						if (error[j] == MPI.SUCCESS) {
							System.out.println("Spawn " + i + " OK");
						}
					}
				}
			}

		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		new Parent(args);
	}
}
