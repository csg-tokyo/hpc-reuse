package csg.chung.mrhpc.utils;

public class LockObj {

	public static boolean LOCK = false;
	
	public static void unlock(){
		LOCK = false;
	}
	
	public static void lock(){
		LOCK = true;
	}	
}
