package csg.chung.mrhpc.multithread;

public class TestThread {

	public static void main(String args[]){
		for (int i=0; i < 1000; i++){
			System.out.println("Create thread #" + i);
			MyThread t = new MyThread();
			t.start();
		}
	}
}
