package csg.chung.mrhpc.deploy.test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public class Test {
	
	public Test(){
			ByteBuffer buf = ByteBuffer.allocate(50); 
			CharBuffer cbuf = buf.asCharBuffer();
			cbuf.put("Java Code Geeks");
			cbuf.flip();
			String s = cbuf.toString();
			System.out.println(s);
	}
	
	public static void main(String args[]){
		new Test();
	}
}
