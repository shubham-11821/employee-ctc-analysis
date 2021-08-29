package com.raj.ctc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CtcMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
	{
		String str[] = values.toString().split("\t");
		context.write(new Text(str[4]), values);
	}
}
