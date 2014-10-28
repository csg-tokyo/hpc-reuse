package csg.chung.mrhpc.processpool;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import javax.print.attribute.standard.RequestingUserName;

import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.Lib;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

public class Pool {
	public final static String HADOOP_FOLDER 			= "/home/mrhpc/hadoop";
	private int rank, size;
	private TaskThread task;
	
	public Pool(){
		try {
			rank = MPI.COMM_WORLD.getRank();
			size = MPI.COMM_WORLD.getSize();
			
			// Print node info
			Lib.printNodeInfo(rank, size);
			if (rank != 0){
				waiting();
			}else{
				request(0);
				for (int i=0; i < 10; i++){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					request(1);
				}	
			}
			
		}catch(MPIException e){
			e.printStackTrace();
		}
	}
	
	public void request(int id){
		try {
			CharBuffer message = ByteBuffer.allocateDirect(500).asCharBuffer();
			String cmd = Integer.toString(id);
			message.put(cmd.toCharArray());	
			Request request = MPI.COMM_WORLD.iSend(message, cmd.toCharArray().length, MPI.CHAR, 5, 99);	
			request.waitFor();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void waiting(){
		try {
			Request request;
			CharBuffer message;		
			message = ByteBuffer.allocateDirect(500).asCharBuffer();			
			request = MPI.COMM_WORLD.iRecv(message, 500, MPI.CHAR, 0, Constants.TAG);
			while (true){
				if (request.test()){
					int cmd = Integer.parseInt(message.toString().trim());
					if (cmd == 0){
						task = new TaskThread(null);
						task.start();
					}else{
						System.out.println("Active: " + task.isAlive());
					}
					message = ByteBuffer.allocateDirect(500).asCharBuffer();			
					request = MPI.COMM_WORLD.iRecv(message, 500, MPI.CHAR, 0, Constants.TAG);					
				}
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public class TaskThread extends Thread{
		String args[];
		
		public TaskThread(String args[]){
			this.args = args;
		}
		
		public void run(){
			try {
				URL[] classpathExt = {new File("/home/mrhpc/deploy/deploy.jar").toURI().toURL()};  
				URLClassLoader loader = new URLClassLoader(classpathExt, null );				
				Class<?> hello = Class.forName("csg.chung.mrhpc.processpool.Task", true, loader);
				Method mainMethod = hello.getMethod("main", String[].class);
				Object[] arguments = new Object[]{args};
				mainMethod.invoke(null, arguments);										
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
	}	
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new Pool();
		MPI.Finalize();
	}	
	
}
