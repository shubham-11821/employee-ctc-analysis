package com.raj.ctc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CtcPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int nReducers) {
		// TODO Auto-generated method stub
		int partition =0;
		String str[] = value.toString().split("\\t");
		String gender = str[3];
		if(nReducers != 0)
		{
			if(gender.equals(("female")))
				partition=0;
			else
				partition=1;
	
		
		}
		return partition;
	}
}
