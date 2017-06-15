package com.sunilapp.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sunilapp.seqfileexample.FormatConverterMapper;



public class WordCountMR extends Configured implements Tool
{
      public int run(String[] args) throws Exception
      {
            //getting configuration object and setting job name
        Configuration conf = getConf();
        conf.setInt("field_pos", 4);
        conf.set("mapreduce.output.textoutputformat.separator",",");
        
        
        Job job = new Job(conf, "Word Count job - "+ args[0]);
       // conf.set("mapreduce.framework.name", "local");
        //mapreduce.framework.name=yarn
        //setting the class names
        
        System.out.println("Aggregation enabled ==>"+conf.get("yarn.log-aggregation-enable"));
        System.out.println("field pos ==> "+conf.getInt("field_pos", 0));
        System.out.println("seperator ==> "+conf.get("mapreduce.output.textoutputformat.separator"));
        
        
        
        job.setJarByClass(WordCountMR.class);
        
        //to accept the hdfs input and outpur dir at run time
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        //setting the combiner class
        job.setCombinerClass(WordCountReducer.class);

        //setting the output data type classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
       

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountMR(), args);
        System.exit(res);
    }
}


