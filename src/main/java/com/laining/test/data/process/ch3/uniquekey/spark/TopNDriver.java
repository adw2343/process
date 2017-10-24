package com.laining.test.data.process.ch3.uniquekey.spark;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.laining.test.data.process.CommandLineUtils;

import scala.Tuple2;

public class TopNDriver {
	static final int DEFAULT_TOP_N = 10;

	public static void main(String[] args) {
		for (String string : args) {
			System.out.println(string);
		}
		Options options = new Options();
		CommandLineUtils.buildCommandLineOptions(options);
		CommandLine commandLine = CommandLineUtils.parseCmdLine("topN", args, options, new PosixParser());
		String path = commandLine.getOptionValue('f');
		int topN = commandLine.hasOption('n') ? Integer.parseInt(commandLine.getOptionValue('n')) : DEFAULT_TOP_N;

		try (JavaSparkContext context = new JavaSparkContext()) {
			final Broadcast<Integer> broadcastTopN = context.broadcast(topN);
			JavaRDD<String> lines = context.textFile(path, 1);
			JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = 1022519871305679107L;

				@Override
				public Tuple2<String, Integer> call(String t) throws Exception {
					String[] tokens = t.split(",", 2);
					if (tokens.length == 2) {
						return new Tuple2<String, Integer>(tokens[1], Integer.parseInt(tokens[0]));
					}
					return null;
				}
			});

			// 为各个输入分区创建本地top n 列表
			JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(
					new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {

						private static final long serialVersionUID = -6207442265867629195L;

						@Override
						public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> t)
								throws Exception {
							SortedMap<Integer, String> sortedMap = new TreeMap<>();
							while (t.hasNext()) {
								Tuple2<String, Integer> tuple2 = t.next();
								if (tuple2 != null) {
									sortedMap.put(tuple2._2(), tuple2._1());
									if (sortedMap.size() > broadcastTopN.getValue()) {
										sortedMap.remove(sortedMap.firstKey());
									}
								}
							}
							return Collections.singletonList(sortedMap).iterator();
						}
					});

			// 创建最终的top n 列表
			SortedMap<Integer, String> finalTopN = finalTop2(partitions, broadcastTopN);
			for (Map.Entry<Integer, String> entry : finalTopN.entrySet()) {
				System.out.println(entry.getKey() + "--" + entry.getValue());
			}

		}

	}

	/**
	 * 使用collect()方法创建最终列表
	 * 
	 * @param partitions
	 * @param broadcastTopN
	 * @return
	 */
	static SortedMap<Integer, String> finalTop1(JavaRDD<SortedMap<Integer, String>> partitions,
			final Broadcast<Integer> broadcastTopN) {
		SortedMap<Integer, String> finalTopN = new TreeMap<>();
		List<SortedMap<Integer, String>> allTopN = partitions.collect();
		for (SortedMap<Integer, String> localTopN : allTopN) {
			for (Map.Entry<Integer, String> entry : localTopN.entrySet()) {
				finalTopN.put(entry.getKey(), entry.getValue());
				if (finalTopN.size() > broadcastTopN.getValue()) {
					finalTopN.remove(finalTopN.firstKey());
				}
			}
		}
		for (Map.Entry<Integer, String> entry : finalTopN.entrySet()) {
			System.out.println(entry.getKey() + "--" + entry.getValue());
		}
		return finalTopN;
	}

	/**
	 * 使用reduce()方法创建最终列表
	 * 
	 * @param partitions
	 * @param broadcastTopN
	 * @return
	 */
	static SortedMap<Integer, String> finalTop2(JavaRDD<SortedMap<Integer, String>> partitions,
			final Broadcast<Integer> broadcastTopN) {
		SortedMap<Integer, String> finalTopN = partitions.reduce(
				new Function2<SortedMap<Integer, String>, SortedMap<Integer, String>, SortedMap<Integer, String>>() {

					private static final long serialVersionUID = 6050474325934411067L;

					@Override
					public SortedMap<Integer, String> call(SortedMap<Integer, String> v1, SortedMap<Integer, String> v2)
							throws Exception {
						SortedMap<Integer, String> topN = new TreeMap<>();
						for (Map.Entry<Integer, String> entry : v1.entrySet()) {
							topN.put(entry.getKey(), entry.getValue());
							if (topN.size() > broadcastTopN.getValue()) {
								topN.remove(topN.firstKey());
							}
						}

						for (Map.Entry<Integer, String> entry : v2.entrySet()) {
							topN.put(entry.getKey(), entry.getValue());
							if (topN.size() > broadcastTopN.getValue()) {
								topN.remove(topN.firstKey());
							}
						}
						return topN;
					}
				});
		return finalTopN;
	}

}
