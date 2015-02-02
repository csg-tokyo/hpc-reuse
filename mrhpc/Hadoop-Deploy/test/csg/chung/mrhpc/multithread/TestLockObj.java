package csg.chung.mrhpc.multithread;

import csg.chung.mrhpc.utils.LockObj;

public class TestLockObj {

	public class MyThread extends Thread{
		
		public int id;
		
		public MyThread(int id){
			this.id = id;
		}
		
		@Override
		public void run(){
			while(LockObj.LOCK){
			}
			
			LockObj.lock();
			for (int i=0; i < 10; i++){
				System.out.println("Thread " + id + " is counting: " + i);
			}
			
			LockObj.unlock();			
		}
	}
	
	public TestLockObj(){
		System.out.println(LockObj.LOCK);
		
		MyThread t1 = new MyThread(1);
		t1.start();
		MyThread t2 = new MyThread(2);		
		t2.start();
		MyThread t3 = new MyThread(3);		
		t3.start();		
	}
	
	public static void main(String args[]){
		new TestLockObj();
	}
}
