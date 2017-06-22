package com.sunilapp.chainmr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpperCaserMapper extends Mapper<Text, IntWritable,Text, IntWritable> {

	public void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
        String word = key.toString().toUpperCase();
        System.out.println("Upper Case:"+word);
        context.write(new Text(word), value);    
    }
}
