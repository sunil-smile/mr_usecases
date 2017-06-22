package com.sunilapp.multimap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class JoinDriver {

	public static int main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);

		if (otherArgs.length != 5) {
			System.exit(-1);
		}

		Job joinJob = Job.getInstance(conf, "Multi Mapper");
		joinJob.setJarByClass(JoinDriver.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(joinJob,new Path(args[0]), TextInputFormat.class, LeftMapper.class);
		MultipleInputs.addInputPath(joinJob,new Path(args[1]), TextInputFormat.class, RightMapper.class);

		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}

		FileOutputFormat.setOutputPath(joinJob, new Path(args[2]));
		
		joinJob.setReducerClass(JoinReducer.class);
		return joinJob.waitForCompletion(true) ? 0 : 1;

		
}

}// end class
