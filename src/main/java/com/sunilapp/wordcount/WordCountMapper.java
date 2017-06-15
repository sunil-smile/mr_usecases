package com.sunilapp.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import com.sunilapp.customdatatype.WebLogWritableComparable;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	Integer pos=0;
	Configuration conf =null;
	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		conf = context.getConfiguration();
		pos=conf.getInt("field_pos", 0);	
		
	}

	// hadoop supported data types
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// taking one line at a time and tokenizing the same
		String line = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		System.out.println("key --> "+key);
		System.out.println("value --> "+value);
		System.out.println("position --> "+pos);

		// iterating through all the words available in that line and forming
		// the key value pair
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			// sending to output collector which in turn passes the same to
			// reducer

			context.write(word, one);
		}
	}
}