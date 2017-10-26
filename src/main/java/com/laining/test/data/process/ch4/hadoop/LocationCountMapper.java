package com.laining.test.data.process.ch4.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author laining
 *
 */
public class LocationCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().trim().split(",");
		context.write(new Text(tokens[0]), new Text(tokens[1]));
	}
}
