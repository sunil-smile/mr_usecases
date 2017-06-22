package com.sunilapp.multioutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MultiOutputDriver  {
	
	public enum APPLN_COUNTER {
		 
		EVEN_REC_COUNT,
		 
		ODD_REC_COUNT
		 
		};

  public static int main(String[] args) throws Exception {

	  Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.exit(-1);
		}
		
		Job job = Job.getInstance(conf, "MultipleOutputs example");
		
		job.setJarByClass(MultiOutputDriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MultiOutputMapper.class);
				
		MultipleOutputs.addNamedOutput(job, "evenlenrecords", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "oddlenrecords",   TextOutputFormat.class, NullWritable.class, Text.class);
		
		int result=job.waitForCompletion(true) ? 0 : 1;
		
		System.out.println("Even Records :"+job.getCounters().findCounter(APPLN_COUNTER.EVEN_REC_COUNT).getValue());
		System.out.println("Odd Records :"+job.getCounters().findCounter(APPLN_COUNTER.ODD_REC_COUNT).getValue());
		
		return result;
	}

}