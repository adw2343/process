package com.laining.test.data.process.ch4.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
	private PairOfStrings outputKey;
	private PairOfStrings outputValue;

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, PairOfStrings, PairOfStrings>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		if (tokens.length == 3) {
			String userId = tokens[1];
			String productId = tokens[2];
			outputKey = new PairOfStrings(userId, "2");
			outputValue = new PairOfStrings("P", productId);
			context.write(outputKey, outputValue);
		}
	}

}
