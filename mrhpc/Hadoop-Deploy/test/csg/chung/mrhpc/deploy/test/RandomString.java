package csg.chung.mrhpc.deploy.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomString {
	
	public RandomString(String output, int line) throws IOException{
		FileWriter fw = new FileWriter(new File(output));
		BufferedWriter out = new BufferedWriter(fw);
		
		for (int i=0; i < line; i++){
			out.write(randomString(26) + "\n");
		}
		
		out.close();
		fw.close();
	}

	public String randomString(int length) {
		String abc = "abcdefghijklmnopqrstuvwxyz".toUpperCase() + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
		char[] chars = abc.toCharArray();
		StringBuilder sb = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < length; i++) {
		    char c = chars[random.nextInt(chars.length)];
		    sb.append(c);
		}
	    return sb.toString();
	}	
	
	public static void main(String args[]) throws NumberFormatException, IOException{
		new RandomString(args[0], Integer.parseInt(args[1]));
	}
}
