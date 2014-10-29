package csg.chung.mrhpc.processpool;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import csg.chung.mrhpc.deploy.Constants;

public class Pool {
	private int rank;
	
	public Pool(int rank){
		this.rank = rank;
		test();
	}
	
	public void test(){
		if (rank == 5){
			int des = rank + 2;
			request(0, des);
			for (int i=0; i < 20; i++){
				try {
					Thread.sleep(1000);
					request(1, des);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}				
	}
	
	public void request(int id, int des) {
		try {
			CharBuffer message = ByteBuffer.allocateDirect(500).asCharBuffer();
			String cmd = Integer.toString(id);
			message.put(cmd.toCharArray());
			Request request = MPI.COMM_WORLD.iSend(message,
					cmd.toCharArray().length, MPI.CHAR, des, Constants.TAG);
			request.waitFor();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
