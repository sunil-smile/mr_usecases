package com.sunilapp.multiinput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiInputMR extends Configured implements Tool
{
	public static class MultiInputMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		String inputFilePath;
		 @Override		 
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			 FileSplit fileSplit = (FileSplit) context.getInputSplit();
			 inputFilePath = fileSplit.getPath().getName().toString();
			 System.out.println(inputFilePath);
			 
			 //if(filenam-emplo)
		   // 0,3,4
			 //sal
			    //0,4,5
		
	}
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
			
			 //context.write(key, value);
			System.out.println(value.toString());
        }
		
	}
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Multi Input");
        
        job.setJarByClass(MultiInputMR.class);
        
        String[] paths = args[0].split(",");
        String inputs = new String();
        
        for(String s : paths){
        	inputs = inputs+s+",";
        }
        inputs = inputs.substring(0, inputs.length()-1);
        System.out.println(inputs);
        
        
        FileInputFormat.setInputPaths(job, inputs);        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);     
        
        job.setMapperClass(MultiInputMapper.class);
        

        //setting the output data type classes
        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(Text.class);     
       

        return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new MultiInputMR(), args);
      System.exit(res);
  }
}



