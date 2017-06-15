package com.sunilapp.customwritable;





import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WCompMapper extends Mapper<LongWritable, Text, CustWritableComparable, IntWritable>
//public class WCompMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
          
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
  	IntWritable empid = new IntWritable() ;
  	Text empname = new Text();
  	Text date = new Text();
  	Text city = new Text();
         
           public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
           {
        	   System.out.println("in map method");
        	   
        		 
        	   String str = value.toString();
               
               System.out.println(str);
               
               String[] splt = str.split(",");
               Integer i = new Integer(splt[0]);
               
             /*  System.out.println(splt[0]);
               System.out.println(splt[1]);
               System.out.println(splt[4]);
               System.out.println(splt[2]); */
               CustWritableComparable wc = new CustWritableComparable(new IntWritable(i), new Text(splt[1]),new Text(splt[4]),new Text(splt[2]));
               context.write(wc, one); 
           }
         
 }





