package edu.upenn.cis.stormlite.bolt;

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
 * A simple adapter that takes a MapReduce "Job" and calls the "map"
 * on a per-tuple basis.
 * 
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

public class MapBolt implements IRichBolt {
	static Logger log = Logger.getLogger(MapBolt.class);

	Job mapJob;
	
	boolean startProcessing = false;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	/**
	 * This tracks how many "end of stream" messages we've seen
	 */
	int neededVotesToComplete = 0;

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    public MapBolt() {
    }
    
	/**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        
        if (!stormConf.containsKey("mapClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("mapClass");
        	
        	try {
				mapJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        
        if (!stormConf.containsKey("spoutExecutors")) {
        	throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        }
        
        // TODO: determine how many end-of-stream requests are needed
        
        int spoutNum = Integer.parseInt(stormConf.get("spoutExecutors"));
        int mapNum = Integer.parseInt(stormConf.get("mapExecutors"));
        String[] workers = WorkerHelper.getWorkers(stormConf);
        int workerNum = workers.length;
      
        this.neededVotesToComplete = spoutNum + spoutNum * mapNum * (workerNum - 1);
        System.out.println("********MapBolt requires EOS number:" + neededVotesToComplete);
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public synchronized void execute(Tuple input) {
    	if (!input.isEndOfStream()) {
    		if(!startProcessing){
    			WorkerServer.setStatus("mapping");
    			WorkerServer.setKeysRead(0);
    			WorkerServer.setKeysWrittern(0);
    			startProcessing = true;
    		}
	        String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);
	        
	        if (neededVotesToComplete == 0)
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        
	        // TODO:  call the mapper, and do bookkeeping to track work done
	        WorkerServer.addKeysRead();
	        mapJob.map(key, value, collector);
	        context.incMapOutputs(key);
	        
    	} else if (input.isEndOfStream()) {
    		// TODO: determine what to do with EOS
    		neededVotesToComplete--;
    		System.out.println("********MapBolt receive one EOS, still need EOS num:" + neededVotesToComplete);
    		if(neededVotesToComplete == 0){
    			System.out.println("********MapBolt receive all needed EOS from filespout,emit EOS itself");
    			collector.emitEndOfStream();
    			WorkerServer.setStatus("waiting");
    		}
    	}
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
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
