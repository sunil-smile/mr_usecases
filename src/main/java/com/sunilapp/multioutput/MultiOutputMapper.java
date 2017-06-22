package com.sunilapp.multioutput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.sunilapp.multioutput.MultiOutputDriver.APPLN_COUNTER;

public class MultiOutputMapper extends
  	Mapper<LongWritable, Text, NullWritable, Text> {

	private MultipleOutputs mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs(context);
		
	}
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int inp_length=value.toString().length();
		if ((inp_length%2)==0) {
			mos.write("evenlenrecords", NullWritable.get(), value);	
			context.getCounter(APPLN_COUNTER.EVEN_REC_COUNT).increment(1);
			//mos.write("evenlenrecords", NullWritable.get(), value,<output path>);	
		}else{
			mos.write("oddlenrecords",  NullWritable.get(), value);
			context.getCounter(APPLN_COUNTER.ODD_REC_COUNT).increment(1);
			//mos.write("oddlenrecords", NullWritable.get(), value,<output path>);
		}

	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}