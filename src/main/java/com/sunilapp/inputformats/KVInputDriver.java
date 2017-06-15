package com.sunilapp.inputformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sunilapp.wordcount.WordCountMR;
import com.sunilapp.wordcount.WordCountMapper;
import com.sunilapp.wordcount.WordCountReducer;

public class KVInputDriver extends Configured implements Tool
{
	public static class KVInputMapper extends Mapper<Text, Text, Text, Text>{
		 public void map(Text key, Text value, Context context) throws IOException, InterruptedException
         {
			 System.out.println("2015");
			 context.write(key, value);
         }
		
	}
	
    public int run(String[] args) throws Exception
    {

      Configuration conf = getConf();
      Job job = Job.getInstance(conf);

      job.setJobName("KV Input format example");
      
      System.out.println("Key Val seperator   ==>"+conf.get("mapreduce.output.textoutputformat.separator"));
      System.out.println("Aggregation enabled ==>"+conf.get("yarn.log-aggregation-enable"));
      
      
      job.setJarByClass(KVInputDriver.class);
      
      //to accept the hdfs input and outpur dir at run time
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setMapperClass(KVInputMapper.class);
     
      //setting the output data type classes
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);    

      return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new KVInputDriver(), args);
      System.exit(res);
  }
}



