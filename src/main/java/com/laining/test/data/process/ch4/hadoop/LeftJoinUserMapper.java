package com.laining.test.data.process.ch4.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
	private PairOfStrings outputKey;
	private PairOfStrings outputValue;

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PairOfStrings, PairOfStrings>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().trim().split(",");
		if (tokens.length == 2) {
			outputKey = new PairOfStrings(tokens[0], "1");
			outputValue = new PairOfStrings("L", tokens[1]);
			context.write(outputKey, outputValue);
		}

	}

}
