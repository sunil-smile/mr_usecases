package com.sunilapp.chainmr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
          int sum = 0;
          /*iterates through all the values available with a key and add them together and give the
          final result as the key and sum of its values*/
          for (IntWritable value : values)
          {
                sum += value.get();

          }
          context.write(key, new IntWritable(sum));
     }
}

