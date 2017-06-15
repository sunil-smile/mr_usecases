package com.sunilapp.mapsidejoin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class JoinMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
	Path[] filepaths =null;
            @Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		filepaths=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for(Path p : filepaths){
   			System.out.println("path -->" + p);   			
   		}	
		
   }

	     
           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
           {
        	   //context.write(NullWritable.get(), new Text());
                  
               
           }
         
 }



