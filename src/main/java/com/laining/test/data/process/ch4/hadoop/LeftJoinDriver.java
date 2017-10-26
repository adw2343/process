package com.laining.test.data.process.ch4.hadoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laining.test.data.process.CommandLineUtils;

public class LeftJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Path transactions = new Path(args[1]); // input
		Path users = new Path(args[0]); // input
		Path output = new Path(args[2]); // output

		Job job = Job.getInstance(getConf());
		job.setJarByClass(LeftJoinDriver.class);
		job.setJobName("LeftJoinDriver");

		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
		job.setReducerClass(LeftJoinReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job, users, TextInputFormat.class, LeftJoinUserMapper.class);
		MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, LeftJoinTransactionMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(PairOfStrings.class);
		FileOutputFormat.setOutputPath(job, output);

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		CommandLineUtils.buildCommandLineOptions(options);
		CommandLine commandLine = CommandLineUtils.parseCmdLine("LeftJoinDriver", args, options, new PosixParser());
		String path = commandLine.getOptionValue('f');
		String outPutPath = commandLine.getOptionValue('o');
		String[] paths = path.split(",");
		System.out.println("inputDir=" + path);
		System.out.println("outputDir=" + outPutPath);
		int returnStatus = ToolRunner.run(new LeftJoinDriver(), new String[] { paths[0], paths[1], outPutPath });
		System.exit(returnStatus);

	}

}
