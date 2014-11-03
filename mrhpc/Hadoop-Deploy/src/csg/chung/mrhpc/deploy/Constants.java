package csg.chung.mrhpc.deploy;

public class Constants {
	public final static int TAG = 99;
	public final static String SPLIT_REGEX = "@@@";
	public final static int ACK_MESSAGE_LENGTH = 5;
	public final static int BYTE_BUFFER_LENGTH = 2048;
	
	// Exchange data between Reducer and Mapper
	public final static int COMMAND_FETCH = 1;
}
