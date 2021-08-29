package com.raj.ctc;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CtcReducer extends Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
	{
		
		int max = Integer.MIN_VALUE;
		String emp = "";
		for(Text t : values) {
			String data[] = t.toString().split("\\t");
			int sal = Integer.parseInt(data[data.length-1]);
			if(max<sal)
			{
				max = sal;
				emp = t.toString();
			}
			
		}
		context.write(NullWritable.get(), new Text(emp));
	}

}
