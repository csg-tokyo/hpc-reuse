package csg.chung.mrhpc.deploy;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Generate {	
	/**
	 * This class generates a job .sh file
	 * 
	 */
	public Generate(){
		try {
			FileWriter fw = new FileWriter(new File("hadoop.sh"));
			BufferedWriter out = new BufferedWriter(fw);
			
			out.write("#!/bin/sh");
			out.write("\n");
			out.write("\n");
			
			out.write("#PJM -L \"rscgrp=debug\"");
			out.write("\n");
			out.write("#PJM -L \"elapse=" + Configure.ELAPSED_TIME + "\"");
			out.write("\n");
			out.write("#PJM -L \"node=" + Configure.NUMBER_OF_NODE + "\"");
			out.write("\n");
			out.write("#PJM -L \"proc-crproc=512\"");
			out.write("\n");
			out.write("#PJM --mpi \"shape=" + (Configure.NUMBER_OF_DATANODE + 1) + "\"");
			out.write("\n");
			out.write("#PJM --mpi \"proc=" + (Configure.NUMBER_OF_DATANODE + 1) + "\"");
			out.write("\n");						
			out.write("#PJM --mpi \"rank-map-bynode\"");
			out.write("\n");
			out.write("\n");
			
			runCommand("tar -zxf " + Configure.OPENMPI_JAVA_LIB + " --directory=" + Configure.DEPLOY_FOLDER);
			out.write("export JAVA_HOME=" + Configure.JAVA_HOME);
			out.write("\n");
			out.write("export OMPIJ_HOME=" + Configure.DEPLOY_FOLDER + "/openmpi");
			out.write("\n");
			out.write("export CLASSPATH=.:${OMPIJ_HOME}/lib/mpi.jar");
			out.write("\n");
			out.write("export LD_LIBRARY_PATH=.:${OMPIJ_HOME}/lib:${LD_LIBRARY_PATH}");
			out.write("\n");
			out.write("export PATH=${OMPIJ_HOME}/bin:${JAVA_HOME}/bin:${PATH}");
			out.write("\n");
			out.write("\n");
			
			out.write("export JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${LD_LIBRARY_PATH}");
			out.write("\n");			
			out.write("export CLASSPATH=.:$CLASSPATH:" + Configure.DEPLOY_JAR);
			out.write("\n");
			out.write("mpirun java csg.chung.mrhpc.deploy.FX10");
			out.write("\n");
			
			out.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Call bash command
	 * @param command
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void runCommand(String command) throws IOException,
			InterruptedException {
		ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c",
				command);
		Process process = processBuilder.start();

		InputStream stderr = process.getErrorStream();
		InputStreamReader isr = new InputStreamReader(stderr);
		BufferedReader br = new BufferedReader(isr);
		String line;
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
		process.waitFor();
	}

	public static void main(String[] args){
		new Generate();
	}
}
