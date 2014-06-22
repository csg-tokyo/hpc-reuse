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
			CharBuffer message = ByteBuffer.allocateDirect(500).asCharBuffer();
			Request request = MPI.COMM_WORLD.iRecv(message, 500, MPI.CHAR, 1, 99);
			
			while (!request.test()){
				//System.out.println("Waiting...");
			}
			
			System.out.println("P" + rank + " received: " + message.toString().trim() + " - " + message.toString().trim().length());			
		}
		if (rank == 1){
			String hello = "Hello World";
			CharBuffer message = ByteBuffer.allocateDirect(500).asCharBuffer();
			message.put(hello.toCharArray());	
			Request request = MPI.COMM_WORLD.iSend(message, hello.toCharArray().length, MPI.CHAR, 0, 99);	
			request.waitFor();
			message.flip();
			System.out.println("P" + rank + " sent: " + message.toString() + " - " + message.toString().length());
		}
	}
	
	public static void main(String args[]) throws MPIException, InterruptedException, UnsupportedEncodingException{
		MPI.Init(args);
		new NonBlocking();
		MPI.Finalize();
	}	
}
