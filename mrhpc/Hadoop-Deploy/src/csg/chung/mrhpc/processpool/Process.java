package csg.chung.mrhpc.processpool;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import csg.chung.mrhpc.deploy.Constants;

public class Process {
	public static final int ACK_OK = 0;
	public static final int ACK_FREE = 1;
	public static final int ACK_BUSY = 2;
	private TaskThread task;
	private int rank;
	private int parent;
	
	public Process(int rank){
		this.rank = rank;
		this.parent = (int)(rank/Startup.NUMBER_PROCESS_EACH_NODE) * Startup.NUMBER_PROCESS_EACH_NODE;
		this.task = null;
	}
	
	public void waiting() {
		try {
			Request request;
			CharBuffer message;
			message = ByteBuffer.allocateDirect(Constants.BYTE_BUFFER_LENGTH).asCharBuffer();
			request = MPI.COMM_WORLD.iRecv(message, Constants.BYTE_BUFFER_LENGTH, MPI.CHAR, parent,
					Constants.TAG);
			while (true) {
				if (request.test()) {
					String split[] = message.toString().trim().split(Constants.SPLIT_REGEX);
					int cmd = Integer.parseInt(split[0]);
					
					if (cmd == Pool.CMD_CHECK_FREE){
						if (task == null || !task.isAlive()){
							sendAck(parent, ACK_FREE);
						}else{
							sendAck(parent, ACK_BUSY);
						}
					}
					
					if (cmd == Pool.CMD_RUN_CLASS){
						task = new TaskThread(split[1], split[2]);
						task.start();	
						sendAck(parent, ACK_OK);
					}
					message = ByteBuffer.allocateDirect(Constants.BYTE_BUFFER_LENGTH).asCharBuffer();
					request = MPI.COMM_WORLD.iRecv(message, Constants.BYTE_BUFFER_LENGTH, MPI.CHAR, parent,
							Constants.TAG);
				}
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void sendAck(int des, int msg){
		int ack[] = new int[1];
		ack[0] = msg;
		try {
			MPI.COMM_WORLD.send(ack, ack.length, MPI.INT, des, Constants.TAG);
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
