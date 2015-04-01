package csg.chung.mrhpc.deploy.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ContainerTime {

	public List<Long> request, load, run, terminate;
	
	public ContainerTime(String input) throws IOException{
		request = new ArrayList<Long>();
		load = new ArrayList<Long>();
		run = new ArrayList<Long>();
		terminate = new ArrayList<Long>();
		
		File dir = new File(input);
		File[] listFiles = dir.listFiles();
		
		for (int i=0; i < listFiles.length; i++){
			if (listFiles[i].getName().startsWith("container")){
				FileReader fr = new FileReader(listFiles[i]);
				BufferedReader read = new BufferedReader(fr);
				
				long start = getLong(read.readLine());
				long requestT = getLong(read.readLine());
				long loadT = getLong(read.readLine());
				long runT = getLong(read.readLine());
				long terminateT = getLong(read.readLine());
				
				if (requestT != -1){
					request.add(requestT - start);
				}else{
					request.add((long) 0);
				}
				
				if (loadT != -1){
					load.add(loadT - requestT);
				}else{
					load.add((long) 0);
				}
				
				if (runT != -1){
					run.add(runT - loadT);
				}else{
					run.add((long) 0);
				}
				
				if (terminateT != -1){
					terminate.add(terminateT - runT);
				}else{
					terminate.add((long) 0);
				}
				
				read.close();
				fr.close();
			}
		}
		
		printOne(request);
		printOne(load);
		printOne(run);
		printOne(terminate);		
	}
	
	public void printOne(List<Long> a){
		for (int i=0; i < a.size(); i++){
			if (i < a.size() - 1){
				System.out.print(a.get(i) + ",");
			}else{
				System.out.print(a.get(i));
			}
		}
		
		System.out.println();
		System.out.println(a.size());
	}
	
	public long getLong(String line){
		if (line == null){
			return -1;
		}
		
		String split[] = line.split(" ");
		
		return Long.parseLong(split[1]);
	}
	
	public static void main(String args[]) throws IOException{
		new ContainerTime("/Users/chung/Desktop/terasort-log");
	}
}
