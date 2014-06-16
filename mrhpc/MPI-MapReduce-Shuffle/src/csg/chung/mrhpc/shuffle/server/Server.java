package csg.chung.mrhpc.shuffle.server;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import com.google.common.base.Charsets;


import mpi.MPI;
import mpi.MPIException;

public class Server {

	private int rank;
	
	protected HttpURLConnection connection;
	  /** Header info of the shuffle http request/response */
	  public static final String HTTP_HEADER_NAME = "name";
	  public static final String DEFAULT_HTTP_HEADER_NAME = "mapreduce";
	  public static final String HTTP_HEADER_VERSION = "version";
	  public static final String DEFAULT_HTTP_HEADER_VERSION = "1.0.0";
	  public static final String HTTP_HEADER_URL_HASH = "UrlHash";
	  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
	  private static final int MAX_ID_LENGTH = 1000;
	  
	public Server() throws Exception{
		rank = MPI.COMM_WORLD.getRank();
		if (rank == 0){
			startServer();
		}
		
		if (rank == 1){
			while(true){
				char[] message = new char[200]; 
				int master = 0;
				MPI.COMM_WORLD.recv(message, 200, MPI.CHAR, master, 99);
				String str = String.valueOf(message).trim();
				System.out.println("Rank " + rank + " received " + str);
				String split[] = str.split("@@@");
				if (split.length > 1){
					getFromHTTP(split[0], split[1]);
				}
			}
		}
	}
	
	public void writeByte(byte[] bFile){
         try { 
	    //convert array of bytes into file
	    FileOutputStream fileOuputStream = 
                  new FileOutputStream("/home/mrhpc/test/memory.txt"); 
	    fileOuputStream.write(bFile);
	    fileOuputStream.close();
 
	    System.out.println("Done");
        }catch(Exception e){
            e.printStackTrace();
        }		
	}
	
	@SuppressWarnings("resource")
	public void getFromHTTP(String link, String key){
		try {
			URL url = new URL(link);
			openConnection(url, key);
			DataInputStream in = new DataInputStream(connection.getInputStream());
			long size = readFields(in);
			byte[] memory;
			BoundedByteArrayOutputStream byteStream;
			byteStream = new BoundedByteArrayOutputStream((int)size);
			memory = byteStream.getBuffer();
			readFully(in, memory, 0, memory.length);
			writeByte(memory);
			String str = new String(memory);
			System.out.println("Memory: " + str + " - " + memory.length);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	  /**
	   * Reads len bytes in a loop.
	   *
	   * @param in InputStream to read from
	   * @param buf The buffer to fill
	   * @param off offset from the buffer
	   * @param len the length of bytes to read
	   * @throws IOException if it could not read requested number of bytes 
	   * for any reason (including EOF)
	   */
	  public static void readFully(InputStream in, byte buf[],
	      int off, int len) throws IOException {
	    int toRead = len;
	    while (toRead > 0) {
	      int ret = in.read(buf, off, toRead);
	      if (ret < 0) {
	        throw new IOException( "Premature EOF from inputStream");
	      }
	      toRead -= ret;
	      off += ret;
	    }
	  }	
	
	  public long readFields(DataInput in) throws IOException {
		    System.out.println("mapID: " + WritableUtils.readStringSafely(in, MAX_ID_LENGTH));
		    System.out.println("compressedLength " + WritableUtils.readVLong(in));
		    long size = WritableUtils.readVLong(in);
		    System.out.println("uncompressedLength " + size);
		    System.out.println("forReduce " + WritableUtils.readVInt(in));
		    return size;
		  }	
	
	protected synchronized void openConnection(URL url, String key) throws IOException {
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		connection = conn;
		
		byte[] encodedKey     = Base64MPI.decode(key, Base64MPI.DEFAULT);
	    SecretKey originalKey = new SecretKeySpec(encodedKey, 0, encodedKey.length, "DES"); //EDIT: missing 'new'		
	
	      String msgToEncode = buildMsgFrom(url);
	      String encHash = hashFromString(msgToEncode,
	          originalKey);
	      
	      // put url hash into http header
	      connection.addRequestProperty(
	          HTTP_HEADER_URL_HASH, encHash);
	    
	    
	      connection.setReadTimeout(1000);
	      // put shuffle version into http header
	      connection.addRequestProperty(HTTP_HEADER_NAME,
	          DEFAULT_HTTP_HEADER_NAME);
	      connection.addRequestProperty(HTTP_HEADER_VERSION,
	          DEFAULT_HTTP_HEADER_VERSION);
		
	}

	  /**
	   * Aux util to calculate hash of a String
	   * @param enc_str
	   * @param key
	   * @return Base64 encodedHash
	   * @throws IOException
	   */
	  public static String hashFromString(String enc_str, SecretKey key) 
	  throws IOException {
	    return generateHash(enc_str.getBytes(Charsets.UTF_8), key); 
	  }	
	
	  /**
	   * Base64 encoded hash of msg
	   * @param msg
	   */
	  public static String generateHash(byte[] msg, SecretKey key) {
	    return new String(Base64.encodeBase64(generateByteHash(msg, key)), 
	        Charsets.UTF_8);
	  }

	  /**
	   * calculate hash of msg
	   * @param msg
	   * @return
	   */
	  private static byte[] generateByteHash(byte[] msg, SecretKey key) {
	    return computeHash(msg, key);
	  }	  
	  
	  /**
	   * Compute HMAC of the identifier using the secret key and return the 
	   * output as password
	   * @param identifier the bytes of the identifier
	   * @param key the secret key
	   * @return the bytes of the generated password
	   */
	  protected static byte[] createPassword(byte[] identifier, 
	                                         SecretKey key) {
	    Mac mac = threadLocalMac.get();
	    try {
	      mac.init(key);
	    } catch (InvalidKeyException ike) {
	      throw new IllegalArgumentException("Invalid key to HMAC computation", 
	                                         ike);
	    }
	    return mac.doFinal(identifier);
	  }
	  
	  private static final ThreadLocal<Mac> threadLocalMac =
			    new ThreadLocal<Mac>(){
			    @Override
			    protected Mac initialValue() {
			      try {
			        return Mac.getInstance(DEFAULT_HMAC_ALGORITHM);
			      } catch (NoSuchAlgorithmException nsa) {
			        throw new IllegalArgumentException("Can't find " + DEFAULT_HMAC_ALGORITHM +
			                                           " algorithm.");
			      }
			    }
			  };	  
	  
	  /**
	   * Compute the HMAC hash of the message using the key
	   * @param msg the message to hash
	   * @param key the key to use
	   * @return the computed hash
	   */
	  public static byte[] computeHash(byte[] msg, SecretKey key) {
	    return createPassword(msg, key);
	  }	  
	  
	  /**
	   * Shuffle specific utils - build string for encoding from URL
	   * @param url
	   * @return string for encoding
	   */
	  public static String buildMsgFrom(URL url) {
	    return buildMsgFrom(url.getPath(), url.getQuery(), url.getPort());
	  }
	  /**
	   * Shuffle specific utils - build string for encoding from URL
	   * @param uri_path
	   * @param uri_query
	   * @return string for encoding
	   */
	  private static String buildMsgFrom(String uri_path, String uri_query, int port) {
	    return String.valueOf(port) + uri_path + "?" + uri_query;
	  }
	
	
	protected synchronized void closeConnection() {
		// Note that HttpURLConnection::disconnect() doesn't trash the object.
		// connect() attempts to reconnect in a loop, possibly reversing this
		if (connection != null) {
			connection.disconnect();
		}
	}
	
	public void startServer() throws Exception{
        String clientSentence;
        String capitalizedSentence;
        @SuppressWarnings("resource")
		ServerSocket welcomeSocket = new ServerSocket(6789);

        while(true){
           Socket connectionSocket = welcomeSocket.accept();
           BufferedReader inFromClient =
              new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
           DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
           clientSentence = inFromClient.readLine();
           capitalizedSentence = clientSentence.toUpperCase() + '\n';
           outToClient.writeBytes(capitalizedSentence);           
           
           System.out.println("Rank " + rank + " received " + clientSentence);
           char[] message = clientSentence.toCharArray();
           MPI.COMM_WORLD.send(message, message.length, MPI.CHAR, 1, 99);
           System.out.println("Rank " + rank + " sent " + clientSentence);
        }		
	}
	
	public static void printInfo(){
		try {
			InetAddress ip = InetAddress.getLocalHost();
			System.out.println("Rank " + MPI.COMM_WORLD.getRank() + ": saying Hello --> " + ip.getHostName() + " - " + ip.getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) throws Exception{
		MPI.Init(args);
		printInfo();
		new Server();
		MPI.Finalize();
	}
}
