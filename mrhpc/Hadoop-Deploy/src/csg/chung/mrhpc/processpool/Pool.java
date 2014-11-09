package csg.chung.mrhpc.processpool;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.Environment;
import csg.chung.mrhpc.utils.Lib;

public class Pool {
	public static final int NO_AVAILABLE_SLOT = -1;
	public static final int CMD_CHECK_FREE = 0;
	public static final int CMD_RUN_CLASS = 1;
	private int rank;
	
	public Pool(int rank){
		this.rank = rank;
	}
	
	public void waiting() {
		try {
			Request request;
			CharBuffer message;
			message = ByteBuffer.allocateDirect(Constants.BYTE_BUFFER_LENGTH).asCharBuffer();
			request = MPI.COMM_WORLD.iRecv(message, Constants.BYTE_BUFFER_LENGTH, MPI.CHAR, MPI.ANY_SOURCE,
					Constants.TAG);
			while (true) {
				if (request.test()) {
					String cmd = message.toString().trim();
					cmd = cmd.replace("default_container_executor.sh", "launch_container.sh");
					System.out.println(rank + " recv: " + cmd);
					startNewProcess(cmd, "");
					
					message = ByteBuffer.allocateDirect(Constants.BYTE_BUFFER_LENGTH).asCharBuffer();
					request = MPI.COMM_WORLD.iRecv(message, Constants.BYTE_BUFFER_LENGTH, MPI.CHAR, MPI.ANY_SOURCE,
							Constants.TAG);
				}
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void startNewProcess(String prop, String className){
		int des = getFreeSLot();
		if (des != NO_AVAILABLE_SLOT){
			request(Lib.buildCommand(CMD_RUN_CLASS, prop, className), des);
			int ack = waitAck(des);
			System.out.println("Ack from " + des + ": " + (ack == Process.ACK_OK ? "Run OK":"Run failed"));			
		}
	}
	
	public int getFreeSLot(){
		for (int i=rank + 1; i < rank + Startup.NUMBER_PROCESS_EACH_NODE; i++){
			request(CMD_CHECK_FREE + "", i);
			int ack = waitAck(i);
			System.out.println("Ack from " + i + ": " + (ack == Process.ACK_FREE ? "Free":"Busy"));
			if (ack == Process.ACK_FREE){
				return i;
			}
		}
		
		return NO_AVAILABLE_SLOT;
	}
	
	public void test(){
		if (rank == 5){
			int des = rank + 2;
			request("0", des);
			for (int i=0; i < 20; i++){
				try {
					Thread.sleep(1000);
					request("1", des);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}				
	}
	
	public void request(String cmd, int des) {
		try {
			CharBuffer message = ByteBuffer.allocateDirect(Constants.BYTE_BUFFER_LENGTH).asCharBuffer();
			message.put(cmd.toCharArray());
			Request request = MPI.COMM_WORLD.iSend(message,
					cmd.toCharArray().length, MPI.CHAR, des, Constants.TAG);
			request.waitFor();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public int waitAck(int des){
		int ack[] = new int[Constants.ACK_MESSAGE_LENGTH];
		try {
			MPI.COMM_WORLD.recv(ack, 1, MPI.INT, des, Constants.TAG);
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ack[0];
	}
}
