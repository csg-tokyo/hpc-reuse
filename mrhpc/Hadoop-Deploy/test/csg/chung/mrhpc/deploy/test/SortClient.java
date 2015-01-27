package csg.chung.mrhpc.deploy.test;

import java.util.Random;

public class SortClient {
	public static final int N = 70000;
	private int a[];
	
	public static int[] random(int n){
		int[] output = new int[n];
		Random rand = new Random();
		for (int i=0; i < n; i++){
			output[i] = rand.nextInt(N);
		}
		
		return output;
	}
		
	public static void printArray(int[] input){
		for (int i=0; i < input.length; i++){
			System.out.print(input[i] + " ");
		}
		System.out.println();
	}
	
	public void sort(int[] input){
		for (int i=0; i < input.length - 1; i++){
			for (int j=i+1; j < input.length; j++){
				if (input[i] > input[j]){
					int tmp = input[i];
					input[i] = input[j];
					input[j] = tmp;
				}
			}
		}
	}
	
	public SortClient(int rank){
		long start = System.currentTimeMillis();
		
		a = random(N);
		sort(a);
		
		System.out.println(rank + " --> Running time: " + (System.currentTimeMillis() - start));
	}
	
	public static void main(String[] args){
		new SortClient(-1);
	}
}
