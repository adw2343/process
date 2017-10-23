package com.laining.test.data.process.ch3.uniquekey.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {// NullWritable是Writable的一个特殊类，实现方法为空实现，不从数据流中读数据，也不写入数据，只充当占位符，如在MapReduce中，如果你不需要使用键或值，你就可以将键或值声明为NullWritable,NullWritable是一个不可变的单实例类型。

	private SortedMap<Double, String> topNcats = new TreeMap<>();
	private int topN = 10; // 默认是取最大的10个

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (Map.Entry<Double, String> entry : topNcats.entrySet()) {
			context.write(NullWritable.get(), new Text(entry.getKey() + "," + entry.getValue()));
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",", 2);
		String keyAsString = tokens[1];
		topNcats.put(Double.parseDouble(tokens[0]), keyAsString);
		if (topNcats.size() > topN)
			topNcats.remove(topNcats.firstKey());
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		topN = configuration.getInt("top.n", 10);
	}

}
