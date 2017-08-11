package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.http.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import test.edu.upenn.cis.stormlite.PrintBolt;
import test.edu.upenn.cis.stormlite.mapreduce.WordFileSpout;

import java.util.HashMap;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  
  private static final String WORD_SPOUT = "WORD_SPOUT";
  private static final String MAP_BOLT = "MAP_BOLT";
  private static final String REDUCE_BOLT = "REDUCE_BOLT";
  private static final String PRINT_BOLT = "PRINT_BOLT";
  
  //workerMap stores worker "IP:port":parameter map for worders
  private HashMap<String,HashMap<String,String>> workerMap = new HashMap<>();
  //keep last access time for each server
  private HashMap<String,Long> lastAceessTime = new HashMap<>();

  public void init(){
	  System.out.println("Master servlet start...");
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
    response.setContentType("text/html");
    String uri =  request.getServletPath();
    PrintWriter out = response.getWriter();
    response.setContentType("text/html");
    out.println("<html><head><title>Master</title></head><body>");
    
    if(uri.equals("/workerstatus")){
    	System.out.println("In workerstatus...");
    	//get request worker's IP address and port
    	String ip = request.getRemoteAddr();
    	String port = request.getParameter("port");
    	String workerKey = ip + ":"+port;
    	//get other parameters and store them in a map
    	HashMap<String,String> param=new HashMap<String, String>();
    	param.put("status", request.getParameter("status"));
    	param.put("job", request.getParameter("job"));
    	param.put("keys_read", request.getParameter("keys_read"));
    	param.put("keys_written", request.getParameter("keys_written"));
    	param.put("results", request.getParameter("results"));
    	
//    	if(!workerMap.containsKey(workerKey)){
    	workerMap.put(workerKey, param);
    	
    	lastAceessTime.put(workerKey,System.currentTimeMillis());
//    	out.println("<p>Parameter set for worker</p>");
//    	System.out.println("Get worker status: port="+port+"\r\njob="+request.getParameter("job")
//    						+"\r\nKeys read ="+ request.getParameter("keys_read")+
//    						"Keys write="+request.getParameter("keys_written")+
//    						"result="+ request.getParameter("results"));
    	
    }else if(uri.equals("/status")){
    	System.out.println("In status");
    	out.println("<html><head><title>Status Page</title></head><body>");
    	out.println("<table><tr>" +
    				"<td>Worker IP:Port</td>" +
    				"<td>Status</td>" +
    				"<td>Job</td>" +
    				"<td>KeysRead</td>" +
    				"<td>KeysWritten</td>"
    				+"<td>Result</td><tr>");
    	for(String worker : workerMap.keySet()){
    		if(System.currentTimeMillis()-lastAceessTime.get(worker)<300000) {
				out.print("<tr>");
				out.println("<td>"+worker+"</td>");
				out.println("<td>"+workerMap.get(worker).get("status")+"</td>");
				out.println("<td>"+workerMap.get(worker).get("job")+"</td>");
				out.println("<td>"+workerMap.get(worker).get("keys_read")+"</td>");
				out.println("<td>"+workerMap.get(worker).get("keys_written")+"</td>");
				out.println("<td>"+workerMap.get(worker).get("results")+"</td>");
				out.print("</tr>");
			}
			else {
				System.out.println("Worker: "+worker+" time out");
				workerMap.remove(worker);
				lastAceessTime.remove(worker);		
			}
    	}
    	out.println("</table>");
    	out.println("</br>");
    	out.println("<h2>Form of Submitting Jobs</h2>");
    	out.println("<form method = \"POST\" name =\"statusform\" action = \"/status\" ><br/>");
    	out.println("Class name of Job: <input type = \"text\" value = \"edu.upenn.cis455.mapreduce.job.WordCount\" name = \"job\"/><br/>");
    	out.println("Input Directory: <input type = \"text\"  value = \"input\" name = \"input\"/><br/>");
    	out.println("Output Directory: <input type = \"text\"  value = \"output\" name = \"output\"/><br/>");
    	out.println("Number of Map Threads: <input type = \"text\" value = \"2\" name = \"numThreadsMap\"/><br/>");
    	out.println("Number of Reduce Threads: <input type = \"text\" value = \"2\" name = \"numThreadsReduce\"/><br/>");
    	out.println("<input type = \"submit\" value = \"Submit\"/></form>");
    	
    }else if(uri.equals("/shutdown")){
    	out.println("<html>");
    	out.println("<body>");
    	
    	for(String workAddr : workerMap.keySet()){
//    		String url = "http://"+ workAddr + "/shutdown";
//    		URL http_url = new URL(url);
//    		System.out.println(url);
//    		HttpURLConnection conn = (HttpURLConnection)http_url.openConnection();
//			conn.setRequestMethod("GET");
//			conn.setDoOutput(true);
//			conn.getOutputStream();
    		sendJob("http//"+workAddr, "GET", null, "shutdown", "");
    	}
    	out.println("All known workers has been shut down");
    	
    }
    out.println("</body></html>");
    out.flush();
    out.close();
    
  }
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException{
	  String uri = request.getServletPath();
	  if(uri.equals("/status")){
		  System.out.println("In runmapreduce");
		  runMapReduce(request,response);
	  }
  }
 
  
  private void runMapReduce(HttpServletRequest request, HttpServletResponse response) throws IOException{
	  PrintWriter out=response.getWriter();
	  response.setContentType("text/html");
	  //create and set config
	  Config config = new Config();
	  
	  String workerListStr = "";
	  for(String worker : workerMap.keySet()){
		  workerListStr += worker;
		  workerListStr += ",";
	  }
	  System.out.println(workerMap.keySet().toString());
	  workerListStr = workerListStr.substring(0,workerListStr.length() - 1);
	  
	  System.out.println("workerList string:"+workerListStr);
	  
	  //get post parameters 
	  String jobName = request.getParameter("job");
	  String mapperNum = request.getParameter("numThreadsMap");
	  String reducerNum =  request.getParameter("numThreadsReduce");
	  String inputDir = request.getParameter("input");
	  String outputDir = request.getParameter("output");
	  
	  //Initialize config
	  config.put("workerList", workerListStr);
	  config.put("job", jobName);
	  config.put("mapClass", jobName);
      config.put("reduceClass", jobName);
      config.put("input", inputDir);
      config.put("output", outputDir);
     
      // Numbers of executors (per node)
      config.put("spoutExecutors", "1");
      config.put("mapExecutors", mapperNum);
      config.put("reduceExecutors", reducerNum);
	  
	  //create an Topology using FileSpout,MapBolt,ReduceBolt
	  FileSpout spout = new WordFileSpout();
      MapBolt bolt = new MapBolt();
      ReduceBolt bolt2 = new ReduceBolt();
      PrintBolt printer = new PrintBolt();

      TopologyBuilder builder = new TopologyBuilder();

      // Only one source ("spout") for the words
      builder.setSpout(WORD_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));
      
      // Parallel mappers, each of which gets specific words
      builder.setBolt(MAP_BOLT, bolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(WORD_SPOUT, new Fields("value"));
      
      // Parallel reducers, each of which gets specific words
      builder.setBolt(REDUCE_BOLT, bolt2, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));

      // Only use the first printer bolt for reducing to a single point
      builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);
      
      Topology topo = builder.createTopology();
      
      //create new worker job
      WorkerJob job = new WorkerJob(topo, config);
      
      ObjectMapper mapper = new ObjectMapper();
      mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		try {
			String[] workers = WorkerHelper.getWorkers(config);

			int i = 0;
			for (String dest: workers) {
		        config.put("workerIndex", String.valueOf(i++));
				if (sendJob(dest, "POST", config, "definejob", 
						mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() != 
						HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job definition request failed");
				}
			}
			for (String dest: workers) {
				if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != 
						HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job execution request failed");
				}
			}
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	        System.exit(0);
		}        
	  
	  
	  //generate page
	  out.println("<html><body><h2>handle map reduce work now</h2>");
	  out.println("<a href=\"/status\">Back to Status</a>>");
	  out.println("</body></html>");
	  
  }
  static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
		URL url = new URL(dest + "/" + job);
		
		System.out.println("Sending request to " + url.toString());
		
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		
		if (reqType.equals("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");
			
			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else
			conn.getOutputStream();
		
		return conn;
  } 
}
  
