package com.sunilapp.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<CompositeKey, Text, NullWritable, Text>
{
      
    public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	System.out.println(key.toString());
    	for(Text ss : values){
    		context.write(NullWritable.get(),ss);
    		//arraylist.add(ss)
    		//break;
    	}
    	
    	
    }
}