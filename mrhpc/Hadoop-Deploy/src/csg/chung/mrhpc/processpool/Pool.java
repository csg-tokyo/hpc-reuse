package csg.chung.mrhpc.processpool;


import mpi.MPIException;
import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.Lib;
import csg.chung.mrhpc.utils.SendRecv;

public class Pool {
	public static final int NO_AVAILABLE_SLOT = -1;
	private int rank;
	private int[] busyFlag;
	
	public Pool(int rank){
		this.rank = rank;
		busyFlag = new int[Configure.NUMBER_PROCESS_EACH_NODE];
		for (int i=0; i < busyFlag.length; i++){
			busyFlag[i] = 0;
		}
		busyFlag[0] = 1;
	}
	
	public void waiting() {
		try {
			SendRecv sr = new SendRecv();						
			while (true) {
					String cmd = sr.exchangeMsgDes(rank);	
					String split[] = cmd.split(Constants.SPLIT_REGEX);
					if (split.length >= 2){
						System.out.println(rank + " Set free slot");
						setFreeSlot(Integer.parseInt(split[0]) % Configure.NUMBER_PROCESS_EACH_NODE);
					}else{
						cmd = cmd.replace("default_container_executor.sh", "launch_container.sh");
						System.out.println(rank + " recv: " + cmd);
						startNewProcess(cmd, "");
					}					
			}
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setFreeSlot(int s){
		busyFlag[s] = 0;
	}	
	
	public void startNewProcess(String prop, String className){
		int des = getFreeSLot();
		if (des != NO_AVAILABLE_SLOT){
			request(Lib.buildCommand(prop, className), des);
		}
		busyFlag[des] = 1;
	}
	
	public int getFreeSLot(){
		for (int i=0; i < busyFlag.length; i++){
			if (busyFlag[i] == 0){
				return i;
			}
		}
		
		return NO_AVAILABLE_SLOT;
	}
		
	public void request(String cmd, int des) {
		try {			
			SendRecv sr = new SendRecv();
			sr.exchangeMsgSrc(rank, rank + des, cmd);
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
