package csg.chung.mrhpc.multithread;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

public class LockFile {

	public static final String filePath = "/Users/chung/Desktop/lock";

	public class MyThread extends Thread {

		@Override
		public void run() {
			File file = new File(filePath);
			try {
				FileOutputStream fos= new FileOutputStream(file);
				while(true){
					FileLock lock = fos.getChannel().tryLock();
					if (lock != null){
						System.out.println("Get locked");
						while(true){
						
						}
					}else{
						System.out.println("Being locking");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				/*
				while (true) {
					try {
						lock = fos.getChannel().tryLock();
						System.out.println("Holding file");
						Thread.sleep(5000);
						for (;;);
						//lock.release();
						//channel.close();
						//break;
					} catch (OverlappingFileLockException e) {
						System.out.println("File is locked");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						// File is open by someone else
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				*/
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public LockFile() {
		MyThread t1 = new MyThread();
		t1.start();
		//MyThread t2 = new MyThread();
		//t2.start();
	}

	public static void main(String[] args) {
		new LockFile();
	}
}
