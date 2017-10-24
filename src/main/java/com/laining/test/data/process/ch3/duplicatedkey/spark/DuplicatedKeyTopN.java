package com.laining.test.data.process.ch3.duplicatedkey.spark;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.laining.test.data.process.CommandLineUtils;

import scala.Tuple2;

/**
 * 使用takeOrdered进行处理
 * 
 * @author laining
 *
 */
public class DuplicatedKeyTopN {

	static final int DEFAULT_TOP_N = 10;

	public static void main(String[] args) {
		Options options = new Options();
		CommandLineUtils.buildCommandLineOptions(options);
		CommandLine commandLine = CommandLineUtils.parseCmdLine("duplicatedKeyTopN", args, options, new PosixParser());
		String path = commandLine.getOptionValue('f');
		int topN = commandLine.hasOption('n') ? Integer.parseInt(commandLine.getOptionValue('n')) : DEFAULT_TOP_N;

		try (JavaSparkContext context = new JavaSparkContext()) {
			final Broadcast<Integer> broadcast = context.broadcast(topN);
			JavaRDD<String> lines = context.textFile(path, 1);
			lines.saveAsTextFile("/data/duplicatedTopN/1");
			JavaRDD<String> rdds = lines.coalesce(9);
			JavaPairRDD<String, Integer> pairs = rdds.mapToPair(new PairFunction<String, String, Integer>() {

				private static final long serialVersionUID = -6080799783769334273L;

				@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
					String[] tokens = line.split(",");
					return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
				}
			});
			pairs.saveAsTextFile("/data/duplicatedTopN/2");
			// 归约重复键
			JavaPairRDD<String, Integer> uniqueKeys = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

				private static final long serialVersionUID = -3366262689111518482L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1 + v2;
				}
			});
			uniqueKeys.saveAsTextFile("/data/duplicatedTopN/3");
			// 查找最终top n
			List<Tuple2<String, Integer>> topNList = uniqueKeys.takeOrdered(broadcast.getValue(),
					MyTupleComparator.INSTANCE);
			for (Tuple2<String, Integer> entry : topNList) {
				System.out.println(entry._1() + "--" + entry._2());

			}

		}

	}

}
