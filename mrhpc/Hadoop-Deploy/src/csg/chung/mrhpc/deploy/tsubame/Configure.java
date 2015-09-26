package csg.chung.mrhpc.deploy.tsubame;

public class Configure {
	/* Constants defined by User */
	/**
	 * Deploying directory for whole data: source code, storage, logs, and so on.
	 */
	public final static String DEPLOY_FOLDER 			= "/home/usr9/14ITA182/hadoop-origin/deploy";
	/**
	 * Java home path. "/usr/local/java/openjdk7" is JAVA_HOME on FX10.
	 */
	public final static String JAVA_HOME				= "/home/usr9/14ITA182/jdk1.7.0_65";
	/**
	 * Username on FX10
	 */
	public final static String USERNAME					= "c83014";
	
	public final static String MAPREDUCE_JOB = "/home/usr9/14ITA182/hadoop-origin/deploy/app-origin.sh";	
	
	/**
	 * Running time for Hadoop cluster
	 */
	public final static String ELAPSED_TIME				= "1:00:00";
	/**
	 * Number of node used for deploying
	 */
	public final static int NUMBER_OF_NODE				= 5;
	/**
	 * Number of node for data storage. Keep two if you do not know exactly what is it.
	 */
	//public final static int NUMBER_OF_DATANODE			= 2;
}
