package csg.chung.mrhpc.deploy.test;

import java.lang.management.ManagementFactory;
import java.util.Date;

import com.sun.management.OperatingSystemMXBean;

@SuppressWarnings("restriction")
public class CPUUsage {

	public CPUUsage() throws InterruptedException{
		OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
		for(;;){
			System.out.println(new Date());
			System.out.println(osBean.getProcessCpuLoad() * 100);
			System.out.println(osBean.getSystemCpuLoad()  * 100);
			System.out.println(osBean.getTotalPhysicalMemorySize());
			System.out.println(osBean.getFreePhysicalMemorySize());
			Thread.sleep(1000);
			System.out.println();
		}
	}
	
	public static void main(String args[]) throws InterruptedException{
		new CPUUsage();
	}
}
