package com.laining.test.data.process.ch1.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {

	@Override
	public int getPartition(DateTemperaturePair arg0, Text arg1, int arg2) {
		return Math.abs(arg0.getTemperature().hashCode() % arg2);
	}

}
