
package com.sunilapp.custominputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * Reads records by splitting them horizontally based on the specificied begin/end tag.
 */
public class StartStopInputFormat extends FileInputFormat<LongWritable, Text> {

	public static final String START_TAG_KEY = "START TAG";
	public static final String END_TAG_KEY = "END TAG";

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit is,
			TaskAttemptContext tac) {
		return new StartStopRecordReader();
	}

	public static class StartStopRecordReader extends
			RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		@Override
        /**
         * This method takes as arguments the map task’s assigned InputSplit and TaskAttemptContext, and prepares the record reader. For file-based input
         * formats, this is a good place to seek to the byte position in the file to begin reading.
         */
		public void initialize(InputSplit is, TaskAttemptContext tac)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) is;

                  // Retrieve Start Tag and End Tag value
			startTag = tac.getConfiguration().get(START_TAG_KEY)
					.getBytes("utf-8");
			endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");

                  // Split is responsible for all records starting from "startTag" and "endTag" positions
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path file = fileSplit.getPath();

			FileSystem fs = file.getFileSystem(tac.getConfiguration());
			fsin = fs.open(fileSplit.getPath());
			fsin.seek(start);

		}

        /**
          * Like the corresponding method of the InputFormat class, this reads a single key/ value pair and returns true until the data is consumed.
        */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						//buffer.write(startTag); 
						int endTaglength = endTag.toString().length();
						if (readUntilMatch(endTag, true)) {
							value.set(buffer.getData(), 1, (buffer.getLength()-endTaglength)+1);
							key.set(fsin.getPos());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}
        //This methods is used by the framework to give generated key/value pairs to an implementation of Mapper.
		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}
        
		//This methods is used by the framework to give generated key/value pairs to an implementation of Mapper.
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;

		}

		/**
	     * This method is to know how much of the input has the RecordReader consumed i.e. has been processed.
	    */
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}
       
		/**
        * This method is used by the framework for cleanup after there are no more key/value pairs to process.
        */ 
		@Override
		public void close() throws IOException {
			fsin.close();
		}

		/**
	        * Util method to check whether record find the match 
	        */
		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						
						return true;
				} else
					i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}

	}

}
