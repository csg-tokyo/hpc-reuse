package csg.chung.mrhpc.multithread;

public class MyThread extends Thread{

	@Override
	public void run(){
		try {
			Thread.sleep(60*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
