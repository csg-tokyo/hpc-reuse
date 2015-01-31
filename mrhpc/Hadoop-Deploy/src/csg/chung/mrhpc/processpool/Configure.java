package csg.chung.mrhpc.processpool;

public class Configure {
	/**
	 * Running file
	 */
	public final static String HADOOP_TAR_GZ_FILE 		= "/mppxb/c83014/hadoopmpi/deploy/hadoop.tar.gz";
	//public final static String OPENMPI_JAVA_LIB 		= "/mppxb/c83014/hadoopmpi/deploy/openmpi.tar.gz";	
	//public final static String HADOOP_TAR_GZ_FILE 		= "/home/usr9/14ITA182/hadoopmpi/deploy/hadoop.tar.gz";	
	
	/**
	 * Deploying directory for whole data: source code, storage, logs, and so on. 
	 * Note: don't add / in the end of the path
	 */
	public final static String DEPLOY_FOLDER 			= "/mppxb/c83014/hadoopmpi/deploy";
	//public final static String DEPLOY_FOLDER 			= "/home/usr9/14ITA182/hadoopmpi/deploy";	
	/**
	 * Java home path. "/usr/local/java/openjdk7" is JAVA_HOME on FX10.
	 */
	public final static String JAVA_HOME				= "/usr/local/java/openjdk7";
	//public final static String JAVA_HOME				= "/home/usr9/14ITA182/jdk1.7.0_65";	
	/**
	 * Username on FX10
	 */
	public final static String USERNAME					= "c83014";
	//public final static String USERNAME					= "14ITA182";
	
	/**
	 * Running time for Hadoop cluster
	 */
	public final static String ELAPSED_TIME				= "00:30:00";	
	
	public final static int NUMBER_PROCESS_EACH_NODE 	= 8;
}
