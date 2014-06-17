package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.MPI;
import mpi.Info;
import mpi.MPIException;

public class Parent {

	public static void main(String args[]) {
		try {
			MPI.Init(args);
			int rank = MPI.COMM_WORLD.getRank();
			int size = MPI.COMM_WORLD.getSize();
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("Parent " + rank + ": " + ip.getHostName()
					+ " - " + ip.getHostAddress());

			int numberSlaves = size - 1;
			
			if (numberSlaves > 0) {
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
						infos[i].set("host", "slave" + (i+1));
					}

					int procs[] = new int[numberSlaves];
					for (int i = 0; i < numberSlaves; i++) {
						procs[i] = 1;
					}

					for (int i = 0; i < numberSlaves; i++) {
						int error[] = new int[procs[i]];
						MPI.COMM_WORLD.spawn(commands[i], params[i], procs[i], infos[i],
								0, error);
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
