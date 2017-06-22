package com.sunilapp.multimap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<String> leftTable = new ArrayList<String>();
		List<String> rightTable = new ArrayList<String>();
		String leftTableRow;
		String rightTableRow;

		Iterator<Text> valueIterator = values.iterator();

		// step1:
		// this while loop will read the all table input records and put it into
		// record into relevant tables
		while (valueIterator.hasNext()) {
			String strNewRow = valueIterator.next().toString();
			String tableName = strNewRow.split("~", -1)[0];
			if (tableName.equals("LEFT_TABLE")) {
				leftTable.add(strNewRow.substring((strNewRow.indexOf("~") + 1), strNewRow.length()));
			} else if (tableName.equals("RIGHT_TABLE")) {
				rightTable.add(strNewRow.substring((strNewRow.indexOf("~") + 1), strNewRow.length()));
			}
		}

		Iterator<String> leftTableIterator = leftTable.iterator();
		Iterator<String> rightTableIterator;

		while (leftTableIterator.hasNext()) {
			leftTableRow = (String) leftTableIterator.next();
			rightTableIterator = rightTable.iterator();
			while (rightTableIterator.hasNext()) {
				rightTableRow = (String) rightTableIterator.next();
				context.write(NullWritable.get(), new Text(leftTableRow + rightTableRow));
			}
		}
	}// end reduce

}
