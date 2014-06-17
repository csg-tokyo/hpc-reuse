package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.MPI;
import mpi.MPIException;
import mpi.Info;

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

			String params[][] = new String[2][];
			for (int i = 0; i < 2; i++) {
				params[i] = new String[1];
				params[i][0] = "csg.chung.mrhpc.deploy.test.Child";
			}
			
			for (int i = 0; i < 2; i++) {
				System.out.println(params[i][0]);
			}
			
			if (numberSlaves > 0) {
				try {
					String commands[] = new String[numberSlaves];
					for (int i = 0; i < numberSlaves; i++) {
						commands[i] = "java";
					}

					Info infos[] = new Info[numberSlaves];
					for (int i = 0; i < numberSlaves; i++) {
						Info slave = new Info();
						slave.set("host", "slave" + (i + 1));
						infos[i] = slave;
					}

					int procs[] = new int[numberSlaves];
					for (int i = 0; i < numberSlaves; i++) {
						procs[i] = 1;
					}

					int error[] = new int[numberSlaves];

					MPI.COMM_SELF.spawnMultiple(commands, params, procs, infos,
							rank, error);

					if (rank == 0) {
						for (int i = 0; i < numberSlaves; i++) {
							if (error[i] == MPI.SUCCESS) {
								System.out.println("Spawn " + i + " OK");
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
