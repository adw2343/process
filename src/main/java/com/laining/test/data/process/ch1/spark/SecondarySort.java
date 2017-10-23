package com.laining.test.data.process.ch1.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SecondarySort {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: SecondarySort <file>");
			System.exit(1);
		}
		String inputPath = args[0];
		try (final JavaSparkContext context = new JavaSparkContext()) { // 连接到Spark master
			JavaRDD<String> lines = context.textFile(inputPath, 1);
			JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() { // 将每一行数据映射成一个以：yyyy-MM为键，气温为值的键值对

				/**
				 * 
				 */
				private static final long serialVersionUID = -8708656447756711071L;

				public Tuple2<String, Integer> call(String t) throws Exception {
					String[] tokens = t.split(",");
					Integer temperature = Integer.valueOf(tokens[3]);
					String yearMonth = tokens[0] + tokens[1];
					return new Tuple2<String, Integer>(yearMonth, temperature);
				}
			});

			JavaPairRDD<String, Iterable<Integer>> groups = pairs.groupByKey();
			JavaPairRDD<String, Iterable<Integer>> sorted = groups
					.mapValues(new Function<Iterable<Integer>, Iterable<Integer>>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = -2769457623725329620L;

						@Override
						public Iterable<Integer> call(Iterable<Integer> v1) throws Exception {
							List<Integer> newArrayList = new ArrayList<>();
							for (Integer tInteger : v1) {
								newArrayList.add(tInteger);
							}
							Collections.sort(newArrayList);
							return newArrayList;
						}
					});
			List<Tuple2<String, Iterable<Integer>>> output2 = sorted.collect();
			output2.stream().forEach(t -> {
				String yearMonth = t._1;
				Iterable<Integer> temperatures = t._2;
				StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
				if (temperatures != null) {
					for (Integer tem : temperatures) {
						stringJoiner.add(String.valueOf(tem));
					}
				}
				System.out.println(yearMonth + " : " + stringJoiner.toString());
			});
		}

	}

}
