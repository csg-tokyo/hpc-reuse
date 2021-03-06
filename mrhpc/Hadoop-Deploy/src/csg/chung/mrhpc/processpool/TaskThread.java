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
import java.util.ArrayList;
import java.util.List;

import mpi.MPI;

import csg.chung.mrhpc.utils.Constants;
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
	PrintStream stdout;
	PrintStream stderr;
	List<String> setEnvKey, setEnvValue;
	List<String> setPropKey, setPropValue;
	
	public TaskThread(String prop, String name) {
		this.properties = prop;
		this.className = name;
		setSystem(properties);		
		args = new String[10];
	}
	
	public TaskThread(String input){
		// System output
		stdout = System.out;
		stderr = System.err;
		setEnvKey = new ArrayList<String>();
		setEnvValue = new ArrayList<String>();		
		setPropKey = new ArrayList<String>();		
		setPropValue = new ArrayList<String>();				
		
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
		Lib.runCommand("rm -rf " + FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank());
		Lib.runCommand("mkdir " + FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank());		
		System.out.println("User dir:" + System.getProperty("user.dir"));
		if (System.getProperty("user.dir") != null){
			setPropKey.add("user.dir");
			setPropValue.add(System.getProperty("user.dir"));
		}else{
			setPropKey.add("user.dir");
			setPropValue.add(Constants.UNKNOW);			
		}
		System.setProperty("user.dir", FX10.TMP_FOLDER + Lib.getHostname() + "/" + Lib.getRank());
		System.out.println("User dir:" + System.getProperty("user.dir"));
	}
	
	public void lineAnalyze(String line){
		String split[] = line.split(" ");
		if (line.startsWith("export")){
			if (System.getenv(split[1].split("=")[0]) != null){
				setEnvKey.add(split[1].split("=")[0]);
				setEnvValue.add(System.getenv(split[1].split("=")[0]));
			}else{
				setEnvKey.add(split[1].split("=")[0]);
				setEnvValue.add(Constants.UNKNOW);				
			}
			Environment.setenv(split[1].split("=")[0], split[1].split("=")[1].replaceAll("\"", ""), true);
		}else
		if (line.startsWith("exec")){
			for (int i=0; i < split.length; i++){
				exe = split[i];
				startJava(split[i]);
				// Set hostname
				if (System.getProperty("hostname") != null){
					setPropKey.add("hostname");
					setPropValue.add(System.getProperty("hostname"));
				}else{
					setPropKey.add("hostname");
					setPropValue.add(Constants.UNKNOW);			
				}				
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
			if (System.getProperty(values[0].substring(2)) != null){
				setPropKey.add(values[0].substring(2));
				setPropValue.add(System.getProperty(values[0].substring(2)));
			}else{
				setPropKey.add(values[0].substring(2));
				setPropValue.add(Constants.UNKNOW);			
			}			
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
	
	public void resetSetup(){
		// Reset stdout and stderr
		System.setOut(stdout);
		System.setErr(stderr);
		
		// Reset variables
		for (int i=0; i < setEnvKey.size(); i++){
			if (setEnvValue.get(i).equals(Constants.UNKNOW)){
				Environment.unsetenv(setEnvKey.get(i));
			}else{
				Environment.setenv(setEnvKey.get(i), setEnvValue.get(i), true);
			}
		}
		
		// Clear properties
		for (int i=0; i < setPropKey.size(); i++){
			if (setPropValue.get(i).equals(Constants.UNKNOW)){
				System.clearProperty(setPropKey.get(i));
			}else{
				System.setProperty(setPropKey.get(i), setPropValue.get(i));
			}
		}		
	}

	public void run() {
		try {
			long time = System.currentTimeMillis();
			//String path = "/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/etc/hadoop:.:/home/mrhpc/hadoop/etc/hadoop:/home/mrhpc/hadoop/share/hadoop/common/lib/*:/home/mrhpc/hadoop/share/hadoop/common/*:/home/mrhpc/hadoop/share/hadoop/hdfs:/home/mrhpc/hadoop/share/hadoop/hdfs/lib/*:/home/mrhpc/hadoop/share/hadoop/hdfs/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/lib/*:/home/mrhpc/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/contrib/capacity-scheduler/*.jar:/home/mrhpc/usr/lib/mpi.jar:/home/mrhpc/test:/home/mrhpc/test/guava-17.0.jar:/home/mrhpc/test/commons-codec-1.9.jar:/home/mrhpc/hadoop/share/hadoop/yarn/*:/home/mrhpc/hadoop/share/hadoop/yarn/lib/*:/home/mrhpc/hadoop/etc/hadoop/nm-config/log4j.properties";
			//int parent = (int)(MPI.COMM_WORLD.getRank()/Startup.NUMBER_PROCESS_EACH_NODE) * Startup.NUMBER_PROCESS_EACH_NODE;
			//String hadoopFolder = FX10.HADOOP_FOLDER + parent; 
			//URL[] classpathExt = buildClasspath(setClasspath(hadoopFolder));
			//URLClassLoader loader = new URLClassLoader(classpathExt, null);
			//String prop = "-Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir= -Dyarn.id.str=mrhpc -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Dyarn.policy.file=hadoop-policy.xml -server -Dhadoop.log.dir=/home/mrhpc/hadoop/logs -Dyarn.log.dir=/home/mrhpc/hadoop/logs -Dhadoop.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.log.file=yarn-mrhpc-nodemanager-slave1.log -Dyarn.home.dir=/home/mrhpc/hadoop -Dhadoop.home.dir=/home/mrhpc/hadoop -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA";
			//"org.apache.hadoop.yarn.server.nodemanager.NodeManager"
			
			//System.out.println(properties + " " + className);
			//System.out.println("Classpath: " + setClasspath(hadoopFolder));
			//System.out.println("Classpath: " + classpathExt.toString());			
			//System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());  			
			Class<?> hello = Class.forName(className);
			System.out.println(MPI.COMM_WORLD.getRank() + " (1)" + " --> " + (System.currentTimeMillis() - time));
			//System.out.println("Free memory 1: " + Runtime.getRuntime().freeMemory());  			
			Method mainMethod = hello.getMethod("main", String[].class);
			System.out.println(MPI.COMM_WORLD.getRank() + " (2)" + " --> " + (System.currentTimeMillis() - time));			
			//System.out.println("Free memory 2: " + Runtime.getRuntime().freeMemory());  						
			Object[] arguments = new Object[] {args};
			mainMethod.invoke(null, arguments);
			System.out.println(MPI.COMM_WORLD.getRank() + " (3)" + " --> " + (System.currentTimeMillis() - time));		
			//System.out.println("Free memory 3: " + Runtime.getRuntime().freeMemory());  						
			//Thread.currentThread().setName("NodeManager");
			//System.out.println(MPI.COMM_WORLD.getRank() + " thread ID: " + Thread.currentThread().getId() + " --> " + (System.currentTimeMillis() - time));
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
