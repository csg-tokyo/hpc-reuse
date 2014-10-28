package csg.chung.mrhpc.deploy.fx10;

public class Configure {
	/* Constants defined by User */
	/**
	 * Deploying directory for whole data: source code, storage, logs, and so on.
	 */
	public final static String DEPLOY_FOLDER 			= "/group1/gc83/c83014/mrhpc/deploy";
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
	public final static int NUMBER_OF_NODE				= 6;
	/**
	 * Number of node for data storage. Keep two if you do not know exactly what is it.
	 */
	//public final static int NUMBER_OF_DATANODE			= 2;
}
