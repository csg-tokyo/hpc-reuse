package csg.chung.mrhpc.deploy.fx10;

public class Configure {
	/* Constants defined by User */
	/**
	 * Deploying directory for whole data: source code, storage, logs, and so on.
	 */
	public final static String DEPLOY_FOLDER 			= "/mppxb/c83014/mrhpc/deploy";
	
	public final static String MAPREDUCE_JOB = "/mppxb/c83014/mrhpc/deploy/app-origin.sh";
	public final static String CPU_LOG = "/mppxb/c83014/mrhpc/deploy/cpu_log_";		
	public final static String ANALYSIS_LOG = "/mppxb/c83014/mrhpc/deploy/log/";	
	
	/**
	 * Java home path. "/usr/local/java/openjdk7" is JAVA_HOME on FX10.
	 */
	public final static String JAVA_HOME				= "/usr/local/java/openjdk7";
	/**
	 * Username on FX10
	 */
	public final static String USERNAME					= "c83014";
	
	/**
	 * Running time for Hadoop cluster
	 */
	public final static String ELAPSED_TIME				= "1:00:00";
	/**
	 * Number of node used for deploying
	 */
	public final static int NUMBER_OF_NODE				= 10;
	/**
	 * Number of node for data storage. Keep two if you do not know exactly what is it.
	 */
	//public final static int NUMBER_OF_DATANODE			= 2;
}
