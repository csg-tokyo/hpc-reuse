package csg.chung.mrhpc.deploy.test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;

public class Test {
	
	public Test() throws MPIException{
			ByteBuffer buf = ByteBuffer.allocate(50); 
			CharBuffer cbuf = buf.asCharBuffer();
			cbuf.put("Java Code Geeks");
			cbuf.flip();
			//String s = cbuf.toString();
			//System.out.println(s);
			
			int rank = MPI.COMM_WORLD.getRank();
			if (rank == 0){
				int length[] = new int[1];
				length[0] = 30;
				MPI.COMM_WORLD.send(length, 1, MPI.INT, 1, 99);
			}else{
				int length[] = new int[1];
				System.out.println("Start receiving");
				MPI.COMM_WORLD.recv(length, 1, MPI.INT, 0, 99);
				System.out.println("Receive " + length[0]);
			}
	}
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new Test();
		MPI.Finalize();
	}
}
