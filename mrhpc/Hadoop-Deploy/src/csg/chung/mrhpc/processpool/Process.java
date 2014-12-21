package csg.chung.mrhpc.processpool;

import mpi.MPIException;
import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.SendRecv;

public class Process {
	private int rank;

	public Process(int rank) {
		this.rank = rank;
	}

	public void waiting() {
		TaskThread t = null;

		try {
			SendRecv sr = new SendRecv();
			String msg = sr.exchangeMsgDes(rank);
			String split[] = msg.split(Constants.SPLIT_REGEX);
			if (split.length >= 2) {
				t = new TaskThread(split[0], split[1]);
				t.start();
			} else {
				t = new TaskThread(split[0]);
				t.start();
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
