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
	
	private int rank;
	
	public Hadoop(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			
			// Print node info
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("P" + rank + ": " + ip.getHostName() + " - " + ip.getHostAddress());	
			
			//initialize(rank);
			
			if (rank == 0){
				// Master node
				Info master = new Info();
				master.set("host", "master");
				master.set("soft", "soft_limits");
				
				Info slave1 = new Info();
				slave1.set("host", "slave1");
				slave1.set("soft", "soft_limits");	
				startMaster(rank, master, slave1);
				
			}else{
				// Slaves node
				//startSlaves(rank);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (MPIException e) {
			e.printStackTrace();
		}
	}
	
	public void startMaster(int rank, Info master, Info slave1){
		try {
			//String commands[] = {"hadoop-daemon.sh", "yarn-daemon.sh", "hadoop-daemon.sh", "yarn-daemon.sh"};
	        //String params[][] = {{"start", "namenode"}, {"start", "resourcemanager"}, {"start", "datanode"}, {"start", "nodemanager"}};

			String commands[] = {"nodemanager.sh"};
	        String params[][] = {};			
			
			int procs[] = {1};
	        Info infos[] = {slave1};
	        MPI.COMM_SELF.spawnMultiple(commands, params, procs, infos, rank, null);	
			System.out.println("Start Master " + rank + " --> OK");				
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public void startSlaves(int rank, Info info){
		try {
			String commands[] = {"hadoop-daemon.sh", "yarn-daemon.sh"};
	        String params[][] = {{"start", "datanode"}, {"start", "nodemanager"}};
	        int procs[] = {1, 1};
	        Info infos[] = {info, info};
	        System.out.println("Rank " + rank + ": " + info.get("host"));
	        MPI.COMM_WORLD.spawnMultiple(commands, params, procs, infos, rank, null);				
			System.out.println("Start Slave " + rank + " --> OK");				
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
