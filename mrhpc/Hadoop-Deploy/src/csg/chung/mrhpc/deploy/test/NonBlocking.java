package csg.chung.mrhpc.deploy.test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

public class NonBlocking {

	public NonBlocking() throws MPIException, InterruptedException, UnsupportedEncodingException{
		int rank = MPI.COMM_WORLD.getRank();
		if (rank == 0){
			ByteBuffer message = ByteBuffer.allocateDirect(500);
			Request request = MPI.COMM_WORLD.iRecv(message, 500, MPI.BYTE, 1, 99);
			
			while (!request.test()){
				//System.out.println("Waiting...");
			}
			
			byte bytes[] = new byte[message.limit()];
			message.get(bytes, 0, bytes.length);
			String des = new String(bytes);
			System.out.println("P" + rank + " received: " + des.trim() + " - " + des.trim().length() + " - " + message.limit());			
		}
		if (rank == 1){
			String hello = "Hello World";
			ByteBuffer message = ByteBuffer.allocateDirect(500);
			message.put(hello.getBytes());	
			Request request = MPI.COMM_WORLD.iSend(message, hello.getBytes().length, MPI.BYTE, 0, 99);	
			request.waitFor();
			message.flip();
			byte bytes[] = new byte[message.limit()];
			message.get(bytes, 0, bytes.length);
			String src = new String(bytes);
			System.out.println("P" + rank + " sent: " + src.trim() + " - " + src.trim().length() + " - " + message.limit());
		}
	}
	
	public static void main(String args[]) throws MPIException, InterruptedException, UnsupportedEncodingException{
		MPI.Init(args);
		new NonBlocking();
		MPI.Finalize();
	}	
}
