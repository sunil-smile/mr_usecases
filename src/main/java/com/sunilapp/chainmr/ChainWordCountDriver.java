package com.sunilapp.chainmr;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainWordCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// JobConf conf = new JobConf(getConf(), ChainWordCountDriver.class);
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Chain WordCount Driver");

		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		// It will delete the output directory if it already exists. don't need
		// to delete it manually
		fs.delete(outputPath,true);

		// Setting the input and output path
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Considering the input and output as text file set the input & output
		// format to TextInputFormat
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		ChainMapper.addMapper(job, TokenizerMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class,conf);
		ChainMapper.addMapper(job, UpperCaserMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class,conf);
		ChainReducer.setReducer(job, WordCountReducer.class, Text.class, IntWritable.class, Text.class,IntWritable.class, conf);
		ChainReducer.addMapper(job, LastMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class,conf);
		
		 
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ChainWordCountDriver(), args);
		System.exit(res);
	}
}
