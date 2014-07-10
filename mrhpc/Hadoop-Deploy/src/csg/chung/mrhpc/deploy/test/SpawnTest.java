package csg.chung.mrhpc.deploy.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import mpi.Info;
import mpi.MPI;
import mpi.MPIException;

public class SpawnTest {
	private int rank, size, numberSlaves;
	
	public SpawnTest() throws MPIException, UnknownHostException{
		rank = MPI.COMM_WORLD.getRank();
		size = MPI.COMM_WORLD.getSize();
		numberSlaves = size - 1;
		
		InetAddress ip = InetAddress.getLocalHost();
		System.out.println("Parent " + rank + ": " + ip.getHostName()
				+ " - " + ip.getHostAddress());		
		
		testSpawn();
	}
	
	public void testSpawn() throws MPIException{
		System.out.println("Rank: " + rank + " starts spawning");
		String params[] = {"csg.chung.mrhpc.deploy.test.SpawnChild"};
		Info info = new Info();
		info.set("host", "localhost");
		MPI.COMM_WORLD.spawn("java", params, 1, info, 0, null);
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
				params[i][0] = "csg.chung.mrhpc.deploy.test.SpawnChild";
			}

			Info infos[] = new Info[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				infos[i] = new Info();
				//infos[i].set("host", "b06tio171");
			}

			int procs[] = new int[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				procs[i] = 1;
			}

			// Spawn each command separately
			for (int i = 0; i < numberSlaves; i++) {
				int error[] = new int[procs[i]];
				MPI.COMM_WORLD.spawn(commands[i], params[i],
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
	
	public static void main(String args[]) throws MPIException, UnknownHostException{
		MPI.Init(args);
		new SpawnTest();
		MPI.Finalize();
	}
}
