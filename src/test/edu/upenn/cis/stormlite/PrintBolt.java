package test.edu.upenn.cis.stormlite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();
	FileWriter fileWriter;
	String outputDir;
	File outDir;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input) {
		if (!input.isEndOfStream()){
			System.out.println(getExecutorId() + ": " + input.toString());
			try{
				FileWriter fw = new FileWriter(outputDir + "/output.txt",true);
				BufferedWriter bw = new BufferedWriter(fw);
				String key = input.getStringByField("key");
				String value = input.getStringByField("value");
				WorkerServer.addResult(key + "," + value);
				bw.write(key + "," + value + "\r\n");
				bw.close();
				WorkerServer.setStatus("idle");
			}catch(IOException e) {
				e.printStackTrace();
			}
		}	
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		outputDir = WorkerServer.storeDir + "/" + stormConf.get("output");
		outDir = new File(outputDir);
		if(!outDir.exists()){
			outDir.mkdir();
		}
		
		File outputFile = new File(outputDir + "/output.txt");
		if(outputFile.exists()){
			outputFile.delete();
		}
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
