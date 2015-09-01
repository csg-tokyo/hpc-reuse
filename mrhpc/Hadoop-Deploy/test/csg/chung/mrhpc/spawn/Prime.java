package csg.chung.mrhpc.spawn;

import mpi.MPI;
import mpi.MPIException;

public class Prime {
	public final static int ISPRIME = 1;
	public final static int NOTPRIME = 0;	
	
	int isPrime(int n){
		int squareroot;
		if (n > 10){
			squareroot = (int) Math.sqrt(n);
			for (int i=3; i <= squareroot; i+=2){
				if (n%i == 0){
					return NOTPRIME;
				}
			}
			
			return ISPRIME;
		}else{
			return NOTPRIME;
		}
	}
	
	public Prime(int maxNumber) throws MPIException{
		int rank, tasks;
		int myStart, gap;
		int[] prime = new int [1];
		int[] largestPrime = new int[1];
		int[] primeCount = new int[1];
		int[] countSum = new int[1];
		
		rank = MPI.COMM_WORLD.getRank();
		tasks = MPI.COMM_WORLD.getSize();
		
		myStart = rank + rank + 1;
		gap = tasks + tasks;
		primeCount[0] = 0;
		prime[0] = 0;
		largestPrime[0] = 0;
		countSum[0] = 0;
		
		if (rank == 0){
			primeCount[0] = 4;
			for (int i = myStart; i <= maxNumber; i+= gap){
				if (isPrime(i) == ISPRIME){
					primeCount[0]++;
					prime[0] = i;
				}
			}
			
			MPI.COMM_WORLD.reduce(prime, largestPrime, 1, MPI.INT, MPI.MAX, 0);
			MPI.COMM_WORLD.reduce(primeCount, countSum, 1, MPI.INT, MPI.SUM, 0);	
			
			System.out.println("Largest prime: " + largestPrime[0]);
			System.out.println("Total prime: " + countSum[0]);			
		}else{
			for (int i = myStart; i <= maxNumber; i+= gap){
				if (isPrime(i) == ISPRIME){
					primeCount[0]++;
					prime[0] = i;
				}
			}			

			MPI.COMM_WORLD.reduce(prime, largestPrime, 1, MPI.INT, MPI.MAX, 0);
			MPI.COMM_WORLD.reduce(primeCount, countSum, 1, MPI.INT, MPI.SUM, 0);		
		}
	}
	
	public static void main(String args[]) throws MPIException{
		MPI.Init(args);
		new Prime(Integer.parseInt(args[0]));
		MPI.Finalize();
	}
}
