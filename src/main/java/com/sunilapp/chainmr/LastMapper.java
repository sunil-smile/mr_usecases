package com.sunilapp.chainmr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class LastMapper extends Mapper<Text, IntWritable,Text, IntWritable> {
	Integer threshold_value=0;
	Configuration conf =null;
	protected void setup(
			Mapper<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		conf = context.getConfiguration();
		threshold_value=conf.getInt("threshold_value", 0);	
		
	}
	public void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
       if(value.get() > threshold_value){
    	   context.write(key, value);
       }
    }
}
