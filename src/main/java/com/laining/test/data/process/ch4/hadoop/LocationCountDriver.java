package com.laining.test.data.process.ch4.hadoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laining.test.data.process.CommandLineUtils;

/**
 * 
 * @author laining
 *
 */
public class LocationCountDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		CommandLineUtils.buildCommandLineOptions(options);
		CommandLine commandLine = CommandLineUtils.parseCmdLine("LocationCountDriver", args, options,
				new PosixParser());
		String path = commandLine.getOptionValue('f');
		String outPutPath = commandLine.getOptionValue('o');
		System.out.println("inputDir=" + path);
		System.out.println("outputDir=" + outPutPath);
		int returnStatus = ToolRunner.run(new LocationCountDriver(), new String[] { path, outPutPath });
		System.exit(returnStatus);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(LocationCountDriver.class);
		job.setJobName("Phase-2: LocationCountDriver");

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(LocationCountMapper.class);
		job.setReducerClass(LocationCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileOutputFormat.setOutputPath(job, output);
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}
}
