package csg.chung.mrhpc.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Lib {
	public static void printNodeInfo(int rank, int size){
		try {
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("P" + rank + "/" + size + ": " + ip.getHostName() + " - " + ip.getHostAddress());						
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
