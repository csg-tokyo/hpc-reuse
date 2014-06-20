package csg.chung.mrhpc.deploy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;

import mpi.Info;
import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;

public class Hadoop {
	public final static String HADOOP_FOLDER 			= "/home/mrhpc/hadoop";
	private Intercomm spawn[];
	private int rank, size, numberSlaves;
	
	public Hadoop(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize();
			numberSlaves = size - 1;
			spawn = new Intercomm[numberSlaves];
			
			// Print node info
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("P" + rank + "/" + size + ": " + ip.getHostName() + " - " + ip.getHostAddress());	
						
			if (rank == 0){
				// Master node
				startMaster(rank);
			}else{
				// Slaves node
				startSlaves(rank);
			}
			
			// Spawn
			if (numberSlaves > 0) {
				spawnOnSlaves(rank, numberSlaves);
				while (true) {
					for (int i = 0; i < numberSlaves; i++) {
						receiveSpawnFromChild(spawn[i], 0, "slave" + (i + 1));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (MPIException e) {
			e.printStackTrace();
		}
	}
	
	public void startMaster(int rank){
		runCommand(HADOOP_FOLDER + "/sbin/hadoop-daemon.sh start namenode");
		runCommand(HADOOP_FOLDER + "/sbin/yarn-daemon.sh start resourcemanager");
		System.out.println("Start Master " + rank + " --> OK");		
	}
	
	public void startSlaves(int rank){
		runCommand(HADOOP_FOLDER + "/sbin/hadoop-daemon.sh start datanode");
		System.out.println("Start Slave " + rank + " --> OK");	
	}	
	
	public void spawnOnSlaves(int rank, int numberSlaves){
		try {
			String commands[] = new String[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				commands[i] = "nodemanager.sh";
			}

			String params[][] = new String[numberSlaves][];
			for (int i = 0; i < numberSlaves; i++) {
				params[i] = new String[2];
				params[i][0] = "start";
				params[i][1] = "nodemanager";				
			}

			int procs[] = new int[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				procs[i] = 1;
			}			
			
			Info infos[] = new Info[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				infos[i] = new Info();
				infos[i].set("host", "slave" + (i + 1));
			}

			for (int i = 0; i < numberSlaves; i++) {
				int error[] = new int[procs[i]];
				spawn[i] = MPI.COMM_WORLD.spawn(commands[i], params[i], procs[i], infos[i], 0, error);
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
	
	public void receiveSpawnFromChild(Intercomm group, int child, String host){
		try {
			char[] message = new char[500]; 
			group.recv(message, 500, MPI.CHAR, child, Constants.TAG);
			String cmd = String.valueOf(message).trim();
			
			//System.out.println(host + " start spawning Grand Child");
			spawnGrandChild(cmd, host);
			
			// Spawn
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
	}
	
	public void spawnGrandChild(String cmd, String host){
		try {
			String params[] = {};
			int proc = 1;
			Info info = new Info();
			info.set("host", host);
			int error[] = new int[proc];
			MPI.COMM_WORLD.spawn(cmd, params, proc, info, 0, error);
			if (error[0] == MPI.SUCCESS) {
				System.out.println("Grand child " + host + " Spawned " + " OK");
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}		
	
	/**
	 * Call bash command
	 * @param command
	 */
	public static void runCommand(String command){		
		ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
		Process process;
		try {
			process = processBuilder.start();
			InputStream stderr = process.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			BufferedReader br = new BufferedReader(isr);
			String line;
			while ((line = br.readLine()) != null){
				System.out.println(line);
			}
			process.waitFor();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}			
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new Hadoop();
		MPI.Finalize();
	}
}
