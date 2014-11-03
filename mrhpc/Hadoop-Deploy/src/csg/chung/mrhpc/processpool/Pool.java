package csg.chung.mrhpc.processpool;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.Lib;

public class Pool {
	public static final int NO_AVAILABLE_SLOT = -1;
	public static final int CMD_CHECK_FREE = 0;
	public static final int CMD_RUN_CLASS = 1;
	private int rank;
	
	public Pool(int rank){
		this.rank = rank;
		if (rank % Startup.NUMBER_PROCESS_EACH_NODE == 0 && rank > 0){
			String prop = "-Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir= -Dyarn.id.str=mrhpc -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Dyarn.policy.file=hadoop-policy.xml -server -Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir=/home/mrhpc/hadoop -Dhadoop.home.dir=/home/mrhpc/hadoop -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA";
			String className = "org.apache.hadoop.yarn.server.nodemanager.NodeManager";	
			startNewProcess(prop, className);
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
