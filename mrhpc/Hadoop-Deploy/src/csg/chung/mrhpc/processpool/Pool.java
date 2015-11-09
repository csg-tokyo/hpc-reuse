package csg.chung.mrhpc.processpool;


import mpi.MPIException;
import csg.chung.mrhpc.deploy.Constants;
import csg.chung.mrhpc.utils.Lib;
import csg.chung.mrhpc.utils.SendRecv;

public class Pool {
	public static final int NO_AVAILABLE_SLOT = -1;
	private int rank;
	private int[] busyFlag;
	private int[] amFlag; // Flag for appMaster checking, two appMaster cannot run on the same slot
	private int counter;
	
	public Pool(int rank){
		this.rank = rank;
		busyFlag = new int[Configure.NUMBER_PROCESS_EACH_NODE];
		amFlag = new int[Configure.NUMBER_PROCESS_EACH_NODE];
		for (int i=0; i < busyFlag.length; i++){
			busyFlag[i] = 0;
			amFlag[i] = 0;
		}
		busyFlag[0] = 1;
		counter = 0;
	}
	
	public void waiting() {
		try {
			SendRecv sr = new SendRecv();						
			while (true) {
					String cmd = sr.exchangeMsgDes(rank);	
					String split[] = cmd.split(Constants.SPLIT_REGEX);
					if (split.length == 2){
						System.out.println(rank + " Set free slot");
						setFreeSlot(Integer.parseInt(split[0]) % Configure.NUMBER_PROCESS_EACH_NODE);
					}else{
						split[0] = split[0].replace("default_container_executor.sh", "launch_container.sh");
						System.out.println(rank + " recv: " + split[0]);
						startNewProcess(split[0], split[1], split[2]);
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
	
	public void startNewProcess(String arg1, String arg2, String arg3){
		int des = getFreeSLot(arg2);
		if (des != NO_AVAILABLE_SLOT){
			request(Lib.buildCommand(arg1, arg2, arg3), des);
		}
		busyFlag[des] = 1;
		// Check AppMaster
		if (arg2.endsWith("000001")){
			amFlag[des] = 1;
		}
	}
	
	public int getFreeSLot(String containerID){
		for(;;){
			if (busyFlag[counter] == 0 && (amFlag[counter] == 0 || (!containerID.endsWith("000001")))){
				return counter;
			}else{
				counter = (counter+1) % busyFlag.length;
			}
		}
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
