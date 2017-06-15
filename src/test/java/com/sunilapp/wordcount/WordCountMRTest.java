package com.sunilapp.wordcount;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.sunilapp.custominputformat.StartStopInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountMRTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		Mapper mapper = new WordCountMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		WordCountReducer reducer = new WordCountReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	// Mapper Test

	
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("orange orange apple"));
		mapDriver.withOutput(new Text("orange"), new IntWritable(1));
		mapDriver.withOutput(new Text("orange"), new IntWritable(1));
		mapDriver.withOutput(new Text("apple"), new IntWritable(1));
		mapDriver.runTest();
	}

	// Reducer Test

	
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("orange"), values);
		reduceDriver.withOutput(new Text("orange"), new IntWritable(2));
		reduceDriver.runTest();
	}

	// Driver Test

	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(1), new Text("orange orange apple"));
		mapReduceDriver.withOutput(new Text("apple"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("orange"), new IntWritable(2));
		mapReduceDriver.runTest();
	}
	
	/*@Test
	public void testRecordReader() throws IOException, InterruptedException {
	Configuration conf = new Configuration(false);
	conf.set("fs.default.name", "file:///");

	File testFile = new File("../wordcount/resources/input");
	Path path = new Path(testFile.getAbsoluteFile().toURI());
	FileSplit split = new FileSplit(path, 0, testFile.length(), null);
	
	InputFormat inputFormat = ReflectionUtils.newInstance(StartStopInputFormat.class, conf);
	TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
	RecordReader reader = inputFormat.createRecordReader(split, context);
	reader.initialize(split, context);
	System.out.println(reader.getProgress()+"----");
//		
	}*/
}
