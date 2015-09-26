package csg.chung.mrhpc.processpool;

import mpi.MPI;
import mpi.MPIException;
import csg.chung.mrhpc.processpool.Configure;
import csg.chung.mrhpc.processpool.FX10;

public class Tsubame{

	public static void main(String[] args) throws MPIException {
		Configure.setTsubame();
		
		MPI.Init(args);
		new FX10();
		
		System.out.println("Hadoop is READY!!!");
		try {
			String time[] = Configure.ELAPSED_TIME.split(":");
			long elapse = Long.parseLong(time[0]) * 60 * 60 * 1000
					+ Long.parseLong(time[1]) * 60 * 1000
					+ Long.parseLong(time[2]) * 1000;
			Thread.sleep(elapse);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		MPI.Finalize();
	}	
}