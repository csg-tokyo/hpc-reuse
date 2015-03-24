package csg.chung.mrhpc.deploy.test;

import java.util.ArrayList;
import java.util.List;

public class TestMemory {
    
	public static void printMemory(){
        int mb = 1024*1024;
        
        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);		
        System.out.println("##### Finish #####");
	}
	
    public static void main(String [] args) {
    	printMemory();    	
    	List<TestObj> data = new ArrayList<TestObj>();
    	int loop = Integer.parseInt(args[0]);
    	for (int i=0; i < loop; i++){
    		data.add(new TestObj(1000000));
    	}
    	printMemory();
    }
}
