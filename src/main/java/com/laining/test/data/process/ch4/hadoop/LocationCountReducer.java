package com.laining.test.data.process.ch4.hadoop;

import java.io.IOException;
import java.util.StringJoiner;

//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author laining
 *
 */
public class LocationCountReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text productID, Iterable<Text> locations, Context context)
			throws IOException, InterruptedException {

		StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
		int size = 0;
		for (Text location : locations) {
			stringJoiner.add(location.toString().trim());
			size++;
		}
		//
		context.write(productID, new Text("," + stringJoiner.toString() + "," + size));
	}
}
