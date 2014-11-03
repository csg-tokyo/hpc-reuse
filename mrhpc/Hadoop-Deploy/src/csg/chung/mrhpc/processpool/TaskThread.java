package csg.chung.mrhpc.processpool;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TaskThread extends Thread {
	String properties;
	String className;

	public TaskThread(String prop, String name) {
		this.properties = prop;
		this.className = name;
	}

	public void run() {
		try {
			//String path = "/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/etc/hadoop:.:/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/share/hadoop/common/lib/*:/home/mrhpc/hadoop/share/hadoop/common/*:/home/mrhpc/hadoop/share/hadoop/hdfs:/home/mrhpc/hadoop/share/hadoop/hdfs/lib/*:/home/mrhpc/hadoop/share/hadoop/hdfs/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/lib/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/contrib/capacity-scheduler/*.jar:/home/mrhpc/usr/lib/mpi.jar:/home/mrhpc/test:/home/mrhpc/test/guava-17.0.jar:/home/mrhpc/test/commons-codec-1.9.jar:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/etc/hadoop/nm-config/log4j.properties";
			//URL[] classpathExt = buildClasspath(path);
			//URLClassLoader loader = new URLClassLoader(classpathExt, null);
			//String prop = "-Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir= -Dyarn.id.str=mrhpc -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Dyarn.policy.file=hadoop-policy.xml -server -Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir=/home/mrhpc/hadoop -Dhadoop.home.dir=/home/mrhpc/hadoop -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA";
			//"org.apache.hadoop.yarn.server.nodemanager.NodeManager"
			
			//System.out.println(properties + " " + className);
			setSystem(properties);			
			Class<?> hello = Class.forName(className);

			Method mainMethod = hello.getMethod("main", String[].class);
			String args[] = {};
			Object[] arguments = new Object[] {args};
			mainMethod.invoke(null, arguments);
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
	
	public URL[] buildClasspath(String input) throws MalformedURLException {
		List<URL> result = new ArrayList<URL>();
		String libs[] = input.split(":");
		
		for (int i=0; i < libs.length; i++){
			libs[i] = libs[i].replace("*", "");
			libs[i] = libs[i].replace("*.jar", "");			
			File dir = new File(libs[i]);
			File[] listFile = dir.listFiles();
			if (listFile != null){
				System.out.println(libs[i] + ": " + listFile.length);
				for (int j = 0; j < listFile.length; j++) {
					if (listFile[j].getName().endsWith(".jar")) {
						//System.out.println(listFile[j]);
						result.add(listFile[j].toURI().toURL());
					}
				}
			}
		}
		
		return result.toArray(new URL[result.size()]);
	}
}
