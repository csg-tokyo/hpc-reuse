package csg.chung.mrhpc.processpool;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class TaskThread extends Thread {
	String args[];

	public TaskThread(String args[]) {
		this.args = args;
	}

	public void run() {
		try {
			String path = "/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/etc/hadoop:.:/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/share/hadoop/common/lib/*:/home/mrhpc/hadoop/share/hadoop/common/*:/home/mrhpc/hadoop/share/hadoop/hdfs:/home/mrhpc/hadoop/share/hadoop/hdfs/lib/*:/home/mrhpc/hadoop/share/hadoop/hdfs/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/lib/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/contrib/capacity-scheduler/*.jar:/home/mrhpc/usr/lib/mpi.jar:/home/mrhpc/test:/home/mrhpc/test/guava-17.0.jar:/home/mrhpc/test/commons-codec-1.9.jar:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/etc/hadoop/nm-config/log4j.properties";
			URL[] classpathExt = buildClasspath(path);
			URLClassLoader loader = new URLClassLoader(classpathExt, null);
			Class<?> hello = Class.forName(
					"org.apache.hadoop.yarn.server.nodemanager.NodeManager",
					true, loader);

			Method mainMethod = hello.getMethod("main", String[].class);
			Object[] arguments = new Object[] { args };
			mainMethod.invoke(null, arguments);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
						result.add(listFile[j].toURI().toURL());
					}
				}
			}
		}
		
		return result.toArray(new URL[result.size()]);
	}
}
