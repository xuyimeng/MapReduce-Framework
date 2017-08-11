package test.edu.upenn.cis.stormlite.mapreduce;

import java.util.Iterator;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class GroupWords implements Job {
	@Override
	public void map(String key, String value, Context context) {
		context.write(value, value);
	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		int i = 0;
		while (values.hasNext()) {
			context.write(key, "");
			values.next();
		}
		
	}

}
