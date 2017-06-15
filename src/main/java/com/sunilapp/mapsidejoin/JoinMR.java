package com.sunilapp.mapsidejoin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sunilapp.wordcount.WordCountMR;

public class JoinMR extends Configured implements Tool{

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
        Job job = new Job(conf, "Join job - "+ args[0]);
        //conf.set("mapreduce.framework.name", "local");
        //mapreduce.framework.name=yarn
        //setting the class names
        
               
        job.setJarByClass(JoinMR.class);
        job.setMapperClass(JoinMapper.class);
        //job.setReducerClass(JoinReducer.class);
        
        //setting the output data type classes
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        
        DistributedCache.addCacheFile(new URI(args[3]), conf);
        
        //to accept the hdfs input and outpur dir at run time
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        

        return job.waitForCompletion(true)?0:1;

	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JoinMR(), args);
        System.exit(res);
    }

}
