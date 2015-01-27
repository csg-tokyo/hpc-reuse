package csg.chung.mrhpc.multithread;

import csg.chung.mrhpc.utils.Lib;
import csg.chung.mrhpc.utils.SendRecv;
import mpi.MPI;
import mpi.MPIException;

public class Test {

	public Test() throws MPIException, InterruptedException{
		int rank = MPI.COMM_WORLD.getRank();
		int size = MPI.COMM_WORLD.getSize();
		Lib.printNodeInfo(rank, size);
				
		if (rank == 0){
			SendRecv sr = new SendRecv();
			sr.exchangeMsgDes(0);
			sr.exchangeMsgDes(0);
		}
		else if (rank == 1){
			//sr.exchangeMsgSrc(1, 0, "Ta la sieu nhan 0123456789");
			//MPIThread sr = new MPIThread();
			//sr.start();
			//sr.join();
			SendRecv sr1 = new SendRecv();
			sr1.exchangeMsgSrc(1, 0, "Ta la sieu nhan rank 11111");			
		}
		else if (rank == 2){
			SendRecv sr1 = new SendRecv();
			sr1.exchangeMsgSrc(2, 0, "Ta la sieu nhan rank 22222");						
		}
	}
	
	public class MPIThread extends Thread{
		
		public MPIThread(){
			
		}
		
		@Override
		public void run(){
			try {
				SendRecv sr = new SendRecv();
				sr.exchangeMsgSrc(1, 0, "Ta la sieu nhan 0123456789");
			} catch (MPIException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String args[]) throws MPIException, InterruptedException{
		MPI.InitThread(args, MPI.THREAD_SERIALIZED);
		new Test();
		MPI.Finalize();
	}
}
