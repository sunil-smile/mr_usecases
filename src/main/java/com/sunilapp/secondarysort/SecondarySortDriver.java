package com.sunilapp.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool
{
    public int run(String[] args) throws Exception
    {
      Configuration conf = getConf();
      Job job = Job.getInstance(conf, "Secondary Sorting");
      
      job.setJarByClass(SecondarySortDriver.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);     
      
      job.setMapperClass(SecondarySortMapper.class);
      job.setMapOutputKeyClass(CompositeKey.class);
      job.setMapOutputValueClass(Text.class);
      
      job.setReducerClass(SecondarySortReducer.class);  
      
      job.setPartitionerClass(ActualKeyPartitioner.class);
      job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
      job.setSortComparatorClass(CompositeKeyComparator.class);

      //setting the output data type classes
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);     
     

      return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new SecondarySortDriver(), args);
      System.exit(res);
  }
}