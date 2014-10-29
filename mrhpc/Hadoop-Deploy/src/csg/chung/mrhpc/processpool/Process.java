package csg.chung.mrhpc.processpool;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import csg.chung.mrhpc.deploy.Constants;

public class Process {
	private TaskThread task;
	
	public void waiting() {
		try {
			Request request;
			CharBuffer message;
			message = ByteBuffer.allocateDirect(500).asCharBuffer();
			request = MPI.COMM_WORLD.iRecv(message, 500, MPI.CHAR, 5,
					Constants.TAG);
			while (true) {
				if (request.test()) {
					int cmd = Integer.parseInt(message.toString().trim());
					if (cmd == 0) {
						task = new TaskThread(null);
						task.start();
					} else {
						System.out.println("Active: " + task.isAlive());
					}
					message = ByteBuffer.allocateDirect(500).asCharBuffer();
					request = MPI.COMM_WORLD.iRecv(message, 500, MPI.CHAR, 0,
							Constants.TAG);
				}
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
