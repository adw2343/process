package com.laining.test.data.process.ch3.duplicatedkey.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

	private SortedMap<Integer, String> topNcats = new TreeMap<>();
	private int topN = 10; // 默认是取最大的10个

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String valueAsString = value.toString().trim();
			String[] tokens = valueAsString.split(",", 2);
			Integer weight = Integer.parseInt(tokens[0]);
			topNcats.put(weight, tokens[1]);
			if (topNcats.size() > topN)
				topNcats.remove(topNcats.firstKey());
		}
	}

	@Override
	protected void cleanup(Reducer<NullWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		for (Map.Entry<Integer, String> entry : topNcats.entrySet()) {
			context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
		}
	}

	@Override
	protected void setup(Reducer<NullWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		topN = context.getConfiguration().getInt("top.n", 10);
	}

}
