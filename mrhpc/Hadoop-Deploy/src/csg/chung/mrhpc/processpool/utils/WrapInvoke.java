package csg.chung.mrhpc.processpool.utils;

import java.lang.reflect.Method;

import mpi.MPI;

public class WrapInvoke {

	public WrapInvoke(String prop, String className, String args[]){
		long time = System.currentTimeMillis();
		setSystem(prop);
		try {
			Class<?> hello = Class.forName(className);
			System.out.println(MPI.COMM_WORLD.getRank() + " (1)" + " --> " + (System.currentTimeMillis() - time));
			System.out.println("Free memory 1: " + Runtime.getRuntime().freeMemory());  			
			Method mainMethod = hello.getMethod("main", String[].class);
			System.out.println(MPI.COMM_WORLD.getRank() + " (2)" + " --> " + (System.currentTimeMillis() - time));			
			System.out.println("Free memory 2: " + Runtime.getRuntime().freeMemory());  						
			Object[] arguments = new Object[] {args};
			mainMethod.invoke(null, arguments);
			System.out.println(MPI.COMM_WORLD.getRank() + " (3)" + " --> " + (System.currentTimeMillis() - time));		
			System.out.println("Free memory 3: " + Runtime.getRuntime().freeMemory());  						
			Thread.currentThread().setName("NodeManager");
			System.out.println(MPI.COMM_WORLD.getRank() + " thread ID: " + Thread.currentThread().getId() + " --> " + (System.currentTimeMillis() - time));			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setSystem(String input){
		String libs[] = input.split(" ");
		for (int i=0; i < libs.length; i++){
			String values[] = libs[i].split("=");
			System.setProperty(values[0].substring(2), values.length == 1 ? "":values[1]);
		}
	}	
}
