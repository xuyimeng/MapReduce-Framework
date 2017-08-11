package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
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

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);

	
	Job reduceJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEof = false;
	
	boolean startProcess = false;
	
	DBWrapper db;
	String dbPath;
	
	/**
	 * Buffer for state, by key
	 */
	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    
    private TopologyContext context;
    
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }   
        
        dbPath = WorkerServer.DBdir +"/"+ getExecutorId();
        db = new DBWrapper(dbPath);
        

        // TODO: determine how many EOS votes needed
 
        int spoutNum = Integer.parseInt(stormConf.get("spoutExecutors"));
        int mapNum = Integer.parseInt(stormConf.get("mapExecutors"));
        int reduceNum = Integer.parseInt(stormConf.get("reduceExecutors"));
        String[] workers = WorkerHelper.getWorkers(stormConf);
        int workerNum = workers.length;
        
        this.neededVotesToComplete = mapNum + mapNum * reduceNum * (workerNum - 1);
        
        System.out.println("********ReduceBolt requires EOS number:" + neededVotesToComplete);
    }
    
	public void clearDB(File file){
		if(file.isDirectory()){
			for(File sub : file.listFiles()){
				clearDB(sub);
			}
		}
		file.delete();
	}

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized void execute(Tuple input) {
    	if(!startProcess){
			WorkerServer.setStatus("reducing");
			WorkerServer.setKeysRead(0);
			WorkerServer.setKeysWrittern(0);
			startProcess = true;
		}
    	if (sentEof) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
		} else if (input.isEndOfStream()) {
			
			// TODO: only if at EOS do we trigger the reduce operation and output all state
			neededVotesToComplete--;
			System.out.println("********ReduceBolt receive one EOS, still need EOS num:" + neededVotesToComplete);
			if(neededVotesToComplete == 0){
    			System.out.println("********ReduceBolt receive all needed EOS from filespout,emit EOS itself");
//				for(String key : stateByKey.keySet()){
//					reduceJob.reduce(key, stateByKey.get(key).iterator(), collector);
//				}
    			for(String key : db.getWordList()){
    				System.out.println("######## word in "+ dbPath + " is "+key);
    				reduceJob.reduce(key, db.getCountList(key).iterator(), collector);
    			}
    			
				collector.emitEndOfStream();
				db.syncDB();
//				clearDB(new File(dbPath)); //delete database for this thread
				sentEof = true;
			}
			
    	} else {
    		// TODO: this is a plain ol' hash map, replace it with BerkeleyDB
    		
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        
//	        System.out.println("&&&&&&&&&&& reduce receive " + key + " : "+value);
	        log.debug(getExecutorId() + " received " + key + " / " + value);
	        WorkerServer.addKeysRead();
	        
//	        synchronized (stateByKey) {
//		        if (!stateByKey.containsKey(key))
//		        	stateByKey.put(key, new ArrayList<String>());
////		        else{
//		        	log.debug("Adding item to " + key + " / " + stateByKey.get(key).size());
//			    stateByKey.get(key).add(value);
//			    log.debug("Current number of "+key + " /"+stateByKey.get(key).size());
//	        }
	        
	        if(!db.containsWord(key)){
	        	db.addNewWord(key, value);
	        }else{
	        	db.addCountToWord(key, value);
	        }
	        System.out.println("&&&&&&&&&&& add to DB " + key + " : "+value);

	        db.syncDB();     
	        
    	}
    }


    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
//    	db.clearDB();
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
