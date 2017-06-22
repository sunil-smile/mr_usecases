package com.sunilapp.multimap;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RightMapper extends Mapper<Object, Text, Text, Text> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String joinKey = null;
		String[] splitTableRecords = value.toString().split(",", -1);

		joinKey = splitTableRecords[1];
		if ((joinKey != null) && (joinKey.length() != 0)) {
			context.write(new Text(joinKey.trim()), new Text("RIGHT_TABLE~" + value.toString()));

		}

	}// end map

}
