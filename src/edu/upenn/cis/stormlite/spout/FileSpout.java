package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class FileSpout implements IRichSpout {
	static Logger log = Logger.getLogger(FileSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	
	/**
	 * This is a simple file reader
	 */
//	String filename;
	String fileDir;
    BufferedReader reader;
	Random r = new Random();
	Queue<File> fileQueue = new LinkedList<File>();
	
	int fileInc = 0;
	boolean sentEof = false;
	
    public FileSpout() {
//    	filename = getFilename();
    }
    
//    public abstract String getFilename();


    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        
//        	log.debug(getExecutorId() + " opening file reader");
        	
        	// If we have a worker index, read appropriate file among xyz.txt.0, xyz.txt.1, etc.
//        	if (conf.containsKey("workerIndex"))
//        		reader = new BufferedReader(new FileReader(filename + "." + conf.get("workerIndex")));
//        	else
//        		reader = new BufferedReader(new FileReader(filename));
        
        // get file directory
        
    	if(!conf.containsKey("input")){
    		// TOTO send user response
    		System.out.println(conf.toString());
    		throw new RuntimeException("FileSpout config need input directory");
    	}
    	fileDir = WorkerServer.storeDir + "/"+ conf.get("input");
    	log.debug("Starting spout for " + fileDir);
    	
    	// put file to read into queue
    	File inputDir = new File(fileDir);
    	
    	for(File subfile : inputDir.listFiles()){
			if(!subfile.isDirectory()){
				fileQueue.add(subfile);
			}
		}	
    	
    	System.out.println("queue size:"+fileQueue.size());
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	if (reader != null)
	    	try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }

    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     */
    @Override
    public synchronized void nextTuple() {
    	if(!fileQueue.isEmpty()){
    		System.out.println("*******File queue size "+fileQueue.size());
    		File f = fileQueue.poll();
    		fileInc++;
    		int wordInc = 0;
    		try {
				reader = new BufferedReader(new FileReader(f));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
    		if(reader != null && !sentEof){
    			String line;
    			try {
					while((line = reader.readLine()) != null){
						log.debug(getExecutorId() + " read from file "+f+": "+line);
						String[] words = line.split("[ \\t\\,.]");
						
						for(String word : words){
							log.debug(getExecutorId() + " emitting " + word);
							this.collector.emit(new Values<Object>(String.valueOf(fileInc)+":"+String.valueOf(wordInc++), word));
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	}else if(!sentEof){
    		log.info(getExecutorId() + " finished file " + fileDir + " and emitting EOS");
    		this.collector.emitEndOfStream();
	        sentEof = true;
    	}
//    	if (reader != null && !sentEof) {
//	    	try {
//		    	String line = reader.readLine();
//		    	if (line != null) {
//		        	log.debug(getExecutorId() + " read from file " + getFilename() + ": " + line);
//		    		String[] words = line.split("[ \\t\\,.]");
//		
//		    		for (String word: words) {
//		            	log.debug(getExecutorId() + " emitting " + word);
//		    	        this.collector.emit(new Values<Object>(String.valueOf(inx++), word));
//		    		}
//		    	} else if (!sentEof) {
//		        	log.info(getExecutorId() + " finished file " + getFilename() + " and emitting EOS");
//	    	        this.collector.emitEndOfStream();
//	    	        sentEof = true;
//		    	}
//	    	} catch (IOException e) {
//	    		e.printStackTrace();
//	    	}
//    	}
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }


	@Override
	public String getExecutorId() {
		
		return executorId;
	}


	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}
