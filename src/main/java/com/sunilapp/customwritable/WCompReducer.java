package com.sunilapp.customwritable;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
//import org.w3c.dom.Text;

public class WCompReducer extends Reducer<CustWritableComparable, IntWritable, CustWritableComparable ,IntWritable>
//public class WCompReducer extends Reducer<CustWritableComparable, IntWritable, CustWritableComparable ,IntWritable>

{
	
//	static final String datec = "2015-02-14";
	
	IntWritable empid = new IntWritable() ;
	Text empname = new Text();
	Text date = new Text();
	Text city = new Text();
	@Override
	public void reduce(CustWritableComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
    	  System.out.println("in reduce method");
           
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
            int count = 0;
            for (IntWritable value : values)
            {
            	                                     
                  
                	  count = count +  value.get();
                	//  context.write(key, new IntWritable(count));
                	
                	 
              }
            
            String str = key.toString();
            
            System.out.println(str);
            
            String[] splt = str.split(",");
            Integer i = new Integer(splt[0]);
            
            /*System.out.println(splt[0]);
            System.out.println(splt[1]);
            System.out.println(splt[2]);
            System.out.println(splt[3]);*/
            
            
            empname.set(splt[1]); 
            
            
                 
            CustWritableComparable wc = new CustWritableComparable(new IntWritable(i), new Text(splt[1]),new Text(splt[2]),new Text(splt[3]));
                      
            	 context.write(wc, new IntWritable(count));
           
           
            
                  

           
        //    context.write(key, new IntWritable(count));
            
            
            
       }

}



