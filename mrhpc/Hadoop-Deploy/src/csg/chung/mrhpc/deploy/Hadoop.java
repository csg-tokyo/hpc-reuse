package csg.chung.mrhpc.deploy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;

import mpi.Info;
import mpi.MPI;
import mpi.MPIException;

public class Hadoop {
	public final static String HADOOP_FOLDER 			= "/home/mrhpc/hadoop";
	
	public Hadoop(){
		try {
			int rank = MPI.COMM_WORLD.getRank();
			int size = MPI.COMM_WORLD.getSize();
			
			// Print node info
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("P" + rank + "/" + size + ": " + ip.getHostName() + " - " + ip.getHostAddress());	
						
			if (rank == 0){
				// Master node
				startMaster(rank);
				spawnOnSlaves(rank, size - 1);
			}else{
				// Slaves node
				startSlaves(rank);
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

			String params[][] = {};

			Info infos[] = new Info[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				Info slave = new Info();
				slave.set("host", "slave" + (i + 1));
				slave.set("soft", "soft_limits");
				infos[i] = slave;
			}

			int procs[] = new int[numberSlaves];
			for (int i = 0; i < numberSlaves; i++) {
				procs[i] = 1;
			}

			MPI.COMM_SELF.spawnMultiple(commands, params, procs, infos, rank, null);
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
