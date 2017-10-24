package com.laining.test.data.process.ch3.duplicatedkey.hadoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laining.test.data.process.CommandLineUtils;

public class TopNDriver extends Configured implements Tool {
	private static final int DEFAULT_TOP_N = 10;

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		int N = Integer.parseInt(args[0]); // top N
		job.getConfiguration().setInt("top.n", N);
		job.setJobName("topNDriver");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		// reduce()'s output (K,V)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		CommandLineUtils.buildCommandLineOptions(options);
		CommandLine commandLine = CommandLineUtils.parseCmdLine("topNDriver", args, options, new PosixParser());
		String path = commandLine.getOptionValue('f');
		String outPutPath = commandLine.getOptionValue('o');
		int topN = commandLine.hasOption('n') ? Integer.parseInt(commandLine.getOptionValue('n')) : DEFAULT_TOP_N;

		System.out.println("N=" + topN);
		System.out.println("inputDir=" + path);
		System.out.println("outputDir=" + outPutPath);
		int returnStatus = ToolRunner.run(new TopNDriver(), new String[] { String.valueOf(topN), path, outPutPath });
		System.exit(returnStatus);

	}

}
