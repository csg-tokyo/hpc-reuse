package csg.chung.mrhpc.utils;

import mpi.MPI;
import mpi.MPIException;
import mpi.Status;

public class SendRecv {
	
	private int rank;
	
	public SendRecv() throws MPIException{
		rank = MPI.COMM_WORLD.getRank();
	}
	
	public void exchangeMsgSrc(int src, int des, String msg) throws MPIException{
		if (rank == src){
			bSendCmd(Constants.EXCHANGE_MSG_CMD, msg.length(), des, Constants.EXCHANGE_MSG_TAG);
			bSendString(msg, des, Constants.DATA_TAG);
			//int cmd[] = bRecvCmd(des, Constants.ACK_TAG);
			//if (cmd[0] == Constants.ACK_CMD){
			//	System.out.println(src + " received ack: " + Constants.MEANING[cmd[0]]);
			//}
		}		
	}
	
	public String exchangeMsgDes(int des) throws MPIException{
		if (rank == des){
			int[] cmd = bRecvCmd(MPI.ANY_SOURCE, Constants.EXCHANGE_MSG_TAG);
			System.out.println(des + " received a command from " + cmd[2] + ": " + Constants.MEANING[cmd[0]]);
			if (cmd[0] == Constants.EXCHANGE_MSG_CMD){
				String data = bRecvString(cmd[1], cmd[2], Constants.DATA_TAG);
				System.out.println(des + " received data from " + cmd[2] + ": " + data);
				//bSendCmd(Constants.ACK_CMD, 0, cmd[2], Constants.ACK_TAG);
				//System.out.println(rank + " sending ack OK");
				return data;
			}
		}		
		
		return null;
	}
	
	public void bSendCmd(int cmd, int length, int proc, int tag) throws MPIException{
		int[] array = new int[2];
		array[0] = cmd;
		array[1] = length;
		MPI.COMM_WORLD.send(array, array.length, MPI.INT, proc, tag);
	}
	
	public int[] bRecvCmd(int proc, int tag) throws MPIException{
		int[] array = new int[3];
		Status status = MPI.COMM_WORLD.recv(array, 2, MPI.INT, proc, tag);
		array[2] = status.getSource();
		
		return array;
	}
	
	public void bSendString(String s, int proc, int tag) throws MPIException{
		char[] charArray = s.toCharArray();
		MPI.COMM_WORLD.send(charArray, charArray.length, MPI.CHAR, proc, tag);
	}
	
	public String bRecvString(int length, int proc, int tag) throws MPIException{
		char[] charArray = new char[length];
		MPI.COMM_WORLD.recv(charArray, length, MPI.CHAR, proc, tag);
		
		return String.valueOf(charArray);
	}
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		
		SendRecv sr = new SendRecv();
		sr.exchangeMsgSrc(1, 0, "one");
		sr.exchangeMsgSrc(2, 0, "twotwo");
		sr.exchangeMsgSrc(3, 0, "threethreethree");		
		sr.exchangeMsgSrc(4, 0, "fourfourfourfour");		
		sr.exchangeMsgSrc(5, 0, "fivefivefivefivefive");				
		
		MPI.Finalize();
	}
}
