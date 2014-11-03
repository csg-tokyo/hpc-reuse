package csg.chung.mrhpc.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import csg.chung.mrhpc.deploy.Constants;

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
	
	/**
	 * Call bash command
	 * @param command
	 */
	public static void runCommand(String command){		
		ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
		Process process;
		try {
			process = processBuilder.start();
			InputStream stderr = process.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			BufferedReader br = new BufferedReader(isr);
			String line;
			while ((line = br.readLine()) != null){
				System.out.println(line);
			}
			process.waitFor();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}		
	
	public static String buildCommand(int cmd, String... args){
		String result = Integer.toString(cmd);
		for (int i=0; i < args.length; i++){
			result = result + Constants.SPLIT_REGEX + args[i];
		}
		return result;
	}	
}
