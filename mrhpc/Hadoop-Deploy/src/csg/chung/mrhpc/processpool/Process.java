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
	private int rank;
	private int parent;
	private int numberThread;
	
	public Process(int rank){
		this.rank = rank;
		this.parent = (int)(rank/Configure.NUMBER_PROCESS_EACH_NODE) * Configure.NUMBER_PROCESS_EACH_NODE;
		this.numberThread = 0;
	}
	
	public void waiting() {
		TaskThread t = null;
		
		try {
			Request request;
			CharBuffer message;
			message = ByteBuffer.allocateDirect(Constants.BYTE_BUFFER_LENGTH).asCharBuffer();
			request = MPI.COMM_WORLD.iRecv(message, Constants.BYTE_BUFFER_LENGTH, MPI.CHAR, parent,
					Constants.TAG);
			numberThread = getCurrentNumberThread();
			while (true) {
				if (request.test()) {
					String split[] = message.toString().trim().split(Constants.SPLIT_REGEX);
					int cmd = Integer.parseInt(split[0]);
					
					if (cmd == Pool.CMD_CHECK_FREE){
						if (getCurrentNumberThread() <= numberThread){
							sendAck(parent, ACK_FREE);
						}else{
							sendAck(parent, ACK_BUSY);
						}
					}
					long time = System.currentTimeMillis();
					if (cmd == Pool.CMD_RUN_CLASS){
						if (t != null){
							t.destroy();
						}
						if (split.length >= 3){
							//System.out.println("Start new 1");
							t = new TaskThread(split[1], split[2]);
							t.start();
						}else{
							//System.out.println("Start new 2");							
							t = new TaskThread(split[1]);
							t.start();
						}
						sendAck(parent, ACK_OK);
						break;
					}
					System.out.println(MPI.COMM_WORLD.getRank() + " new TaskThread" + " --> " + (System.currentTimeMillis() - time));
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
	
	public int getCurrentNumberThread(){
		//System.out.println(rank + " number of thread: " + Thread.currentThread().getThreadGroup().activeCount());
		return Thread.currentThread().getThreadGroup().activeCount();		
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
