package csg.chung.mrhpc.deploy;

public class Configure {
	/* Constants defined by User */
	/**
	 * Running file
	 */
	public final static String HADOOP_TAR_GZ_FILE 		= "/mppxb/c83014/hadoopmpi/deploy/hadoop.tar.gz";
	public final static String OPENMPI_JAVA_LIB 		= "/mppxb/c83014/hadoopmpi/deploy/openmpi.tar.gz";	
	public final static String DEPLOY_JAR 				= "/mppxb/c83014/hadoopmpi/deploy/deploy.jar";
	
	/**
	 * Deploying directory for whole data: source code, storage, logs, and so on. 
	 * Note: don't add / in the end of the path
	 */
	public final static String DEPLOY_FOLDER 			= "/mppxb/c83014/hadoopmpi/deploy";
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
	public final static String ELAPSED_TIME				= "00:30:00";
	/**
	 * Number of node used for deploying
	 */
	public final static int NUMBER_OF_NODE				= 10;
	/**
	 * Number of node for data storage. Keep two if you do not know exactly what is it.
	 */
	public final static int NUMBER_OF_DATANODE			= 1;
	
	public final static int NUMBER_OF_NODEMANAGER		= 1;
}
