package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import edu.upenn.cis.stormlite.TopologyContext;

public class WorkerStatusReporter extends Thread{
	String masterAddr;
	int portNum;
	boolean isRunning;
	TopologyContext context;
	
	public WorkerStatusReporter(String addr,int port){
		this.masterAddr = addr;
		this.portNum = port;
		this.isRunning = true;
	}
	
	public void run(){
		while(isRunning) {
			String url = "http://"+ masterAddr + "/workerstatus";
			String query = "port=" + portNum +"&status="+WorkerServer.getStatus()
							+ "&job=" + WorkerServer.getJob() 
							+ "&keys_read=" + WorkerServer.getKeysRead() +"&keys_written=" 
							+ WorkerServer.getKeysWritten() + "&results=" 
							+ parseResults(WorkerServer.getResult());
			try {
				
				URL http_url = new URL(url + "?" + query);
				
//				System.out.println(http_url);
				
				HttpURLConnection urlConnection = (HttpURLConnection)http_url.openConnection();
				urlConnection.setRequestMethod("GET");
				urlConnection.setDoOutput(true);
				urlConnection.getResponseCode();
//				urlConnection.getResponseMessage();
			} catch (IOException e) {
				//e.printStackTrace();
				System.out.println("Worker Port: " + portNum + " Reporting status failed");
			}
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	public String parseResults(List<String> results){
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for(String str : results) {
			if(sb.length() != 0) sb.append(",");
			str = str.replaceAll(",", "-");
			sb.append(str);
			count++;
			if(count == 100) break;
		}
		return "[" + sb.toString() + "]";
	}
}
