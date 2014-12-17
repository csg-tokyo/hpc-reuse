package csg.chung.mrhpc.utils;

import csg.chung.mrhpc.deploy.test.SortClient;

public class SortThread extends Thread{

	private int rank;
	
	public SortThread(int rank){
		this.rank = rank;
	}
	
	@Override
	public void run(){
		new SortClient(rank);
	}
}
