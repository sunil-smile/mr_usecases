package com.sunilapp.customwritable;




import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class WCompMR  extends Configured implements Tool
{
      public int run(String[] args) throws Exception
      {
            //getting configuration object and setting job name
            Configuration conf = getConf();
            
            //
        
		Job job = new Job(conf, "Birth Day Count job - "+ args[0]);
        
	 
		
	
        job.setJarByClass(WCompMR.class);
        job.setMapperClass(WCompMapper.class);
     job.setReducerClass(WCompReducer.class);
        
        //setting the combiner class
       // job.setCombinerClass(WCompReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
             

        //setting the output data type classes
        job.setOutputKeyClass(CustWritableComparable.class);
        job.setOutputValueClass(IntWritable.class);
       // job.setMapOutputKeyClass(Text.class);
       
        //to accept the hdfs input and outpur dir at run time
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        
return job.waitForCompletion(true) ? 0 : 1;
    }

      
      
      
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WCompMR(), args);
        System.exit(res);
    }
}




