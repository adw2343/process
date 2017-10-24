package com.laining.test.data.process.ch3.duplicatedkey.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateByKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text k2 = new Text();
	private IntWritable v2 = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().trim().split(",");
		if (tokens.length != 2)
			return;
		k2.set(tokens[0]);
		v2.set(Integer.valueOf(tokens[1]));
		context.write(k2, v2);
	}

}
