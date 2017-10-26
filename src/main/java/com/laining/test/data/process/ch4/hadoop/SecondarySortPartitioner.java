package com.laining.test.data.process.ch4.hadoop;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author laining
 *
 */
public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {
	@Override
	public int getPartition(PairOfStrings key, Object value, int numberOfPartitions) {
		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
	}
}
