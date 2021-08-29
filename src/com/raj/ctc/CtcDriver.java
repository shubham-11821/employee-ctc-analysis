package com.raj.ctc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CtcDriver {
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Ctc Analysis");
		job.setJarByClass(CtcDriver.class);
		job.setMapperClass(CtcMapper.class);
		job.setPartitionerClass(CtcPartitioner.class);
		job.setReducerClass(CtcReducer.class);
		job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullPointerException.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/raj/DF/Research/MapReduce/CtcAnalysis/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/raj/DF/Research/MapReduce/CtcAnalysis/output"));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
