package edu.upenn.cis455.mapreduce.job;

import java.util.Iterator;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
    // Your map function for WordCount goes here
	// key: document name, line no
	// value: contents of line
//	  System.out.println("******Map Receive key:"+key+" Value:"+value);
	  String[] words = value.split(" ");
	  for(String word : words){
		  word = word.trim();
		  if(!word.equals("")){
			  context.write(word, "1");
			  WorkerServer.addKeysWritten();
		  }
	  }

  }
  
  public void reduce(String key, Iterator<String> values, Context context)
  {
    // Your reduce function for WordCount goes here
	  int result = 0;
	  while(values.hasNext()){
		  result += Integer.parseInt(values.next());
	  }
	  context.write(key, String.valueOf(result));
	  WorkerServer.addKeysWritten();
  }
  
}
