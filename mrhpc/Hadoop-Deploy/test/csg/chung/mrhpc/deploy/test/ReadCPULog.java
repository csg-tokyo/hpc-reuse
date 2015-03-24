package csg.chung.mrhpc.deploy.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class ReadCPULog {

	public static void printList(List<Double> input){
		for (int i=0; i < input.size(); i++){
			System.out.print(input.get(i) + ",");
		}
		System.out.println();
	}
	
	public static List<Double> ReadFile(String input, int limit) throws IOException{
		List<Double> result = new ArrayList<Double>();
		
		FileReader fr = new FileReader(new File(input));
		BufferedReader read = new BufferedReader(fr);
		
		String line;
		int count = 0;
		while ((line = read.readLine()) != null){
			count = (count+1) % 6;
			if (count == 3){
				DecimalFormat df = new DecimalFormat("#.##"); 
				String data = df.format(Double.parseDouble(line));
				result.add(Double.valueOf(data));
			}
			
			if (result.size() >= limit){
				break;
			}
		}
		
		read.close();
		fr.close();
		
		return result;
	}
	
	public static List<Double> addTwoList(List<Double> a, List<Double> b){
		for (int i=0; i < a.size(); i++){
			a.set(i, a.get(i) + b.get(i));
		}
		
		return a;
	}
	
	public static List<Double> ReadDir(String input, int limit) throws IOException{
		List<Double> result = null;
		File dir = new File(input);
		File[] list = dir.listFiles();
		
		int count = 0;
		
		for (int i=0; i < list.length; i++){
			if (list[i].getName().startsWith("cpu_log")){
				count++;
				List<Double> one = ReadCPULog.ReadFile(list[i].getAbsolutePath(), limit);
				if (result == null){
					result = one;
				}else{
					result = addTwoList(result, one);
				}
			}
		}
		
		for (int i=0; i < result.size(); i++){
			Double avg = result.get(i)/count;
			DecimalFormat df = new DecimalFormat("#.##"); 
			String data = df.format(avg);			
			result.set(i, Double.valueOf(data));
		}
		
		return result;
	}
	
	public static void main(String args[]) throws IOException{
		List<Double> result = ReadCPULog.ReadDir("/Users/chung/Desktop/cpu-jvm", 1800);
		ReadCPULog.printList(result);
		System.out.println(result.size());
	}
}
