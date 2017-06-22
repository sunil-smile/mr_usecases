package com.sunilapp.seqfileexample;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceToTextDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out
          .printf("Two parameters need to be supplied - <input dir> and <output dir>\n");
      return -1;
    }

    Configuration conf = new Configuration();    
    Job job = Job.getInstance(conf, "Sequence to text");
    job.setJarByClass(SequenceToTextDriver.class);
    job.setJobName("Convert Sequence File and Output as Text");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(FormatConverterMapper.class);
    job.setNumReduceTasks(0);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new SequenceToTextDriver(), args);
    System.exit(exitCode);
  }
}
