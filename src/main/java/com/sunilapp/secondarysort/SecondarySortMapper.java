package com.sunilapp.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	CompositeKey compositeKey;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
		String fields[]=value.toString().split(",");
		
		System.out.println(value.toString());
		String uid = fields[0];
		String datetime = fields[4];
		
		compositeKey = new CompositeKey(uid, datetime);
		
		System.out.println(compositeKey.toString());
		context.write(compositeKey, value);      
    }

}

