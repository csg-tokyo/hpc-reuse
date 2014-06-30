/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.InputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import mpi.Intercomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class InMemoryMapOutput<K, V> extends MapOutput<K, V> {
  private static final Log LOG = LogFactory.getLog(InMemoryMapOutput.class);
  private Configuration conf;
  private final MergeManagerImpl<K, V> merger;
  private byte[] memory;
  private BoundedByteArrayOutputStream byteStream;
  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final Decompressor decompressor;

  public InMemoryMapOutput(Configuration conf, TaskAttemptID mapId,
                           MergeManagerImpl<K, V> merger,
                           int size, CompressionCodec codec,
                           boolean primaryMapOutput) {
    super(mapId, (long)size, primaryMapOutput);
    this.conf = conf;
    this.merger = merger;
    this.codec = codec;
    byteStream = new BoundedByteArrayOutputStream(size);
    memory = byteStream.getBuffer();
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
    } else {
      decompressor = null;
    }
  }

  public byte[] getMemory() {
    return memory;
  }

  public BoundedByteArrayOutputStream getArrayStream() {
    return byteStream;
  }

  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    IFileInputStream checksumIn = 
      new IFileInputStream(input, compressedLength, conf);

    input = checksumIn;       
  
    // Are map-outputs compressed?
    if (codec != null) {
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }
  
    try {
      IOUtils.readFully(input, memory, 0, memory.length);
      metrics.inputBytes(memory.length);
      reporter.progress();
      LOG.info("Read " + memory.length + " bytes from map-output for " +
                getMapId());

      /**
       * We've gotten the amount of data we were expecting. Verify the
       * decompressor has nothing more to offer. This action also forces the
       * decompressor to read any trailing bytes that weren't critical
       * for decompression, which is necessary to keep the stream
       * in sync.
       */
      if (input.read() >= 0 ) {
        throw new IOException("Unexpected extra bytes from input stream for " +
                               getMapId());
      }

    } catch (IOException ioe) {      
      // Close the streams
      IOUtils.cleanup(LOG, input);

      // Re-throw
      throw ioe;
    } finally {
      CodecPool.returnDecompressor(decompressor);
    }
  }
  
	public void shuffleMPI(MapHost host, InputStream input, String mapId, long compressedLength,
			long decompressedLength, ShuffleClientMetrics metrics,
			Reporter reporter) throws IOException {
		try {
			// MPI code is inserted here
			try {
				Intercomm parent = Intercomm.getParent();
				InetAddress ip = InetAddress.getLocalHost();
				System.out.println("Fetch from mappers: "
						+ parent.getRemoteSize() + " - " + ip.getHostName());
				LOG.info(host.getHostName());
				int node = Integer.parseInt(host.getHostName().split(":")[0].replace("slave",""));

				String path = "/tmp/hadoop-mrhpc/nm-local-dir/usercache/mrhpc/appcache/" + host.getBaseUrl().split("=")[1].replace("&reduce", "").replace("job", "application")
								+ "/output/" + mapId + "/file.out";
				CharBuffer message = ByteBuffer.allocateDirect(500)
						.asCharBuffer();
				message.put(path.toCharArray());
				Request request = parent.iSend(message,
						path.toCharArray().length, MPI.CHAR, node, 99);
				request.waitFor();
				int length[] = new int[1];
				parent.recv(length, 1, MPI.INT, node, 99);
				LOG.info("Length " + length[0]);
				memory = new byte[length[0]];
				parent.recv(memory, length[0], MPI.BYTE, node, 99);
				//for (int i=0; i < memory.length; i++){
				//	memory[i] = bytes[i];
				//}
			} catch (MPIException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			metrics.inputBytes(memory.length);
			reporter.progress();
			LOG.info("Read " + memory.length + " bytes from map-output for "
					+ getMapId());
			LOG.info("Memory: " + new String(memory));
			/**
			 * We've gotten the amount of data we were expecting. Verify the
			 * decompressor has nothing more to offer. This action also forces
			 * the decompressor to read any trailing bytes that weren't critical
			 * for decompression, which is necessary to keep the stream in sync.
			 */

		} catch (IOException ioe) {
			// Re-throw
			throw ioe;
		} finally {
			CodecPool.returnDecompressor(decompressor);
		}
	}
  
  @Override
  public void commit() throws IOException {
    merger.closeInMemoryFile(this);
  }
  
  @Override
  public void abort() {
    merger.unreserve(memory.length);
  }

  @Override
  public String getDescription() {
    return "MEMORY";
  }
}
