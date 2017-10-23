package com.laining.test.data.process.ch3.uniquekey.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		int N = Integer.parseInt(args[0]); // top N
		job.getConfiguration().setInt("top.n", N);
		job.setJobName("topNDriver");

		// job.setInputFormatClass(SequenceFileInputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("usage TopNDriver <N> <input> <output>");
			System.exit(1);
		}

		System.out.println("N=" + args[0]);
		System.out.println("inputDir=" + args[1]);
		System.out.println("outputDir=" + args[2]);
		int returnStatus = ToolRunner.run(new TopNDriver(), args);
		System.exit(returnStatus);

	}

}
