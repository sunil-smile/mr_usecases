package com.sunilapp.inputformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NLineinputDriver extends Configured implements Tool
{
	public static class NLineinputMapper extends Mapper<LongWritable, Text, Text, Text>{
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
         {
			 context.write(new Text(key.toString()), value);
         }
		
	}
	
    public int run(String[] args) throws Exception
    {

      Configuration conf = getConf();
      Job job = Job.getInstance(conf);
      //conf.setInt("mapreduce.input.lineinputformat.linespermap", 5);

      job.setJobName("KV_Inputformat_example");
      
      System.out.println("Line Format ==>"+conf.get("mapreduce.input.lineinputformat.linespermap"));
      
      
      job.setJarByClass(NLineinputDriver.class);
      
      //to accept the hdfs input and outpur dir at run time
      FileInputFormat.addInputPath(job, new Path(args[0]));
      LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      job.setInputFormatClass(NLineInputFormat.class);      
      job.setMapperClass(NLineinputMapper.class);
     
      //setting the output data type classes
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);    

      return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new NLineinputDriver(), args);
      System.exit(res);
  }
}




