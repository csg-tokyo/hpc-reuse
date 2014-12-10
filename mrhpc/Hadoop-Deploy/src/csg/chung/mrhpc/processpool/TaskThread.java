package csg.chung.mrhpc.processpool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import mpi.MPI;

import csg.chung.mrhpc.utils.Environment;
import csg.chung.mrhpc.utils.Lib;

public class TaskThread extends Thread {
	String properties;
	String className;
	String home;
	boolean startArg;
	String args[];
	int count;
	String exe;
	
	public TaskThread(String prop, String name) {
		this.properties = prop;
		this.className = name;
		setSystem(properties);		
		args = new String[10];
	}
	
	public TaskThread(String input){
		args = new String[10];
		count = 0;
		startArg = false;
		makeJobDir();
		try {
			FileReader fr = new FileReader(new File(input));
			BufferedReader in = new BufferedReader(fr);
			String line;
			in.readLine();
			while ((line=in.readLine()) != null){
				//System.out.println(line);
				lineAnalyze(line);
			}
			in.close();
			fr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(this.className);
	}
	
	public void makeJobDir(){
		home = FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank();
		//System.out.println("Make dir: " + FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank());
		Lib.runCommand("mkdir " + FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank());		
		//System.out.println("User dir:" + System.getProperty("user.dir"));
		System.setProperty("user.dir", FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank());
		System.out.println("User dir:" + System.getProperty("user.dir"));
	}
	
	public void lineAnalyze(String line){
		String split[] = line.split(" ");
		if (line.startsWith("export")){
			Environment.setenv(split[1].split("=")[0], split[1].split("=")[1].replaceAll("\"", ""), false);
		}else
		if (line.startsWith("exec")){
			for (int i=0; i < split.length; i++){
				exe = split[i];
				startJava(split[i]);
				// Set hostname
				System.setProperty("hostname", Lib.getHostname());
			}
		}else{
			if (line.length() > 0){
				Lib.runCommand(line, home);
			}
		}
	}
	
	public void startJava(String str){
		//System.out.println(str);
		if (str.startsWith("-D")){
			String values[] = str.split("=");
			System.setProperty(values[0].substring(2), values.length == 1 ? "":values[1]);			
		}
		if (!str.startsWith("1>") && !str.startsWith("2>") && startArg  == true){
			args[count] = str;
			count++;
		}		
		if (str.startsWith("org")){
			this.className = str;
			startArg = true;
		}
		if (str.startsWith("1>")){
			String values[] = str.split(">");
			try {
				System.setOut(new PrintStream(new File(values[1])));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (str.startsWith("2>")){
			String values[] = str.split(">");
			try {
				System.setErr(new PrintStream(new File(values[1])));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}

	public void run() {
		try {
			long time = System.currentTimeMillis();
			//String path = "/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/etc/hadoop:.:/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/share/hadoop/common/lib/*:/home/mrhpc/hadoop/share/hadoop/common/*:/home/mrhpc/hadoop/share/hadoop/hdfs:/home/mrhpc/hadoop/share/hadoop/hdfs/lib/*:/home/mrhpc/hadoop/share/hadoop/hdfs/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/lib/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/contrib/capacity-scheduler/*.jar:/home/mrhpc/usr/lib/mpi.jar:/home/mrhpc/test:/home/mrhpc/test/guava-17.0.jar:/home/mrhpc/test/commons-codec-1.9.jar:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/etc/hadoop/nm-config/log4j.properties";
			int parent = (int)(MPI.COMM_WORLD.getRank()/Configure.NUMBER_PROCESS_EACH_NODE) * Configure.NUMBER_PROCESS_EACH_NODE;
			String hadoopFolder = FX10.HADOOP_FOLDER + parent; 
			URL[] classpathExt = buildClasspath(setClasspath(hadoopFolder));
			URLClassLoader loader = new URLClassLoader(classpathExt);
			//String prop = "-Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir= -Dyarn.id.str=mrhpc -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Dyarn.policy.file=hadoop-policy.xml -server -Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir=/home/mrhpc/hadoop -Dhadoop.home.dir=/home/mrhpc/hadoop -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA";
			//"org.apache.hadoop.yarn.server.nodemanager.NodeManager"
			
			//System.out.println(properties + " " + className);
			//System.out.println("Classpath: " + setClasspath(hadoopFolder));
			//System.out.println("Classpath: " + classpathExt.toString());			
			System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());  			
			//Class<?> hello = Class.forName(className);
			Class<?> hello = loader.loadClass(className);
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

	public String setClasspath(String home){
		String classpath = 	"/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/etc/hadoop:.:/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/share/hadoop/common/lib/*:/home/mrhpc/hadoop/share/hadoop/common/*:/home/mrhpc/hadoop/share/hadoop/hdfs:/home/mrhpc/hadoop/share/hadoop/hdfs/lib/*:/home/mrhpc/hadoop/share/hadoop/hdfs/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/lib/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/contrib/capacity-scheduler/*.jar:" + 
							"/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/etc/hadoop/nm-config/log4j.properties:";
		classpath = classpath.replaceAll("/home/mrhpc/hadoop", home);
		return classpath;
		//Environment.setenv("CLASSPATH", classpath, true);
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
				//System.out.println(libs[i] + ": " + listFile.length);
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
