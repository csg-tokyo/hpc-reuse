package csg.chung.mrhpc.processpool;

public class TaskTest {

	public TaskTest(){
		int count = 0;
		while (count < 5){
			System.out.println(count++);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String args[]){
		System.out.println("This is the Task class");
		new TaskTest();
	}
}
