package com.laining.test.data.process.ch3.duplicatedkey.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<Text, IntWritable, NullWritable, Text> {// NullWritable是Writable的一个特殊类，实现方法为空实现，不从数据流中读数据，也不写入数据，只充当占位符，如在MapReduce中，如果你不需要使用键或值，你就可以将键或值声明为NullWritable,NullWritable是一个不可变的单实例类型。

	private SortedMap<Integer, String> topNcats = new TreeMap<>();
	private int topN = 10; // 默认是取最大的10个

	@Override
	protected void cleanup(Mapper<Text, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (Map.Entry<Integer, String> entry : topNcats.entrySet()) {
			context.write(NullWritable.get(), new Text(entry.getKey() + "," + entry.getValue()));
		}
	}

	@Override
	protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String keyAsString = key.toString();
		int frequency = value.get();
		topNcats.put(frequency, keyAsString);
		// keep only top N
		if (topNcats.size() > topN) {
			topNcats.remove(topNcats.firstKey());
		}
	}

	@Override
	protected void setup(Mapper<Text, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		topN = configuration.getInt("top.n", 10);
	}

}
