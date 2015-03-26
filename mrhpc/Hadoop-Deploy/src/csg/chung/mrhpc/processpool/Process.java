package csg.chung.mrhpc.processpool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Date;

import mpi.MPIException;
import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.Lib;
import csg.chung.mrhpc.utils.SendRecv;

public class Process {
	private int rank;

	public Process(int rank) {
		this.rank = rank;
	}

	public void waiting() {
		TaskThread t = null;
		try {
			for (;;) {
				SendRecv sr = new SendRecv();
				String msg = sr.exchangeMsgDes(rank);
				String split[] = msg.split(Constants.SPLIT_REGEX);
				if (split.length == 2) {
					// For nodemanager
					t = new TaskThread(split[0], split[1]);
					t.start();
					break;
				} else {
					// loaded log
					String logDate = "load: " + new Date().getTime();
					csg.chung.mrhpc.utils.Lib.appendToFile(csg.chung.mrhpc.processpool.Configure.ANALYSIS_LOG + split[1], logDate);
					
		    		  File file1 = new File(csg.chung.mrhpc.processpool.Configure.LOCK_FILE_PATH + split[1]);
		    		  if (!file1.exists()){
		    			  file1.createNewFile();
		    		  }
		    		  FileOutputStream fos= new FileOutputStream(file1);

		    		  FileLock lock1 = fos.getChannel().tryLock();					
					t = new TaskThread(split[0]);
					t.start();
					t.join();
					File file = new File(
							csg.chung.mrhpc.processpool.Configure.LOCK_FILE_PATH
									+ rank);
					if (!file.exists()) {
						try {
							file.createNewFile();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					FileChannel channel = new RandomAccessFile(file, "rw")
							.getChannel();
					FileLock lock;
					while (true) {
						try {
							lock = channel.tryLock();

							// Ok. You get the lock
							lock.release();
							channel.close();
							break;
						} catch (OverlappingFileLockException e) {
							// File is open by someone else
							try {
								Thread.sleep(100);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					String free = Lib.buildCommand(Integer.toString(rank),
							Integer.toString(rank));
					int parent = (int) (rank / Configure.NUMBER_PROCESS_EACH_NODE)
							* Configure.NUMBER_PROCESS_EACH_NODE;
					SendRecv srFree = new SendRecv();
					srFree.exchangeMsgSrc(rank, parent, free);
					
					t.resetSetup();
					
  			      lock1.release();
  			      fos.close();		
  			      
					// finishing log
					logDate = "finishing: " + new Date().getTime();  
					csg.chung.mrhpc.utils.Lib.appendToFile(csg.chung.mrhpc.processpool.Configure.ANALYSIS_LOG + split[1], logDate);  			      
				}
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}	
	}
}
