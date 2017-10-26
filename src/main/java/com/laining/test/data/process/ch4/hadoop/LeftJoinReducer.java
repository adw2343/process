package com.laining.test.data.process.ch4.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

	private static Logger THE_LOGGER = LoggerFactory.getLogger("DefaultLogger");

	@Override
	protected void reduce(PairOfStrings arg0, Iterable<PairOfStrings> arg1,
			Reducer<PairOfStrings, PairOfStrings, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<PairOfStrings> iterator = arg1.iterator();
		String location = "undefined";
		THE_LOGGER.info("phase1:key->" + arg0.getLeftElement());
		while (iterator.hasNext()) {
			PairOfStrings firstValue = iterator.next();
			THE_LOGGER.info(
					"phase1:first value->[" + firstValue.getLeftElement() + "," + firstValue.getRightElement() + "]");
			if ("L".equals(firstValue.getLeftElement())) {
				location = firstValue.getRightElement();
			}
			while (iterator.hasNext()) {
				PairOfStrings productValue = iterator.next();
				THE_LOGGER.info("phase1:other value->[" + productValue.getLeftElement() + ","
						+ productValue.getRightElement() + "]");
				if ("P".equals(productValue.getLeftElement())) {
					context.write(new Text(productValue.getRightElement() + ","), new Text(location));
				}
			}
		}

	}

}
