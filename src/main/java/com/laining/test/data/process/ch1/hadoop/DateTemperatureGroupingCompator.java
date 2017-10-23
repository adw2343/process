package com.laining.test.data.process.ch1.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingCompator extends WritableComparator {

	public DateTemperatureGroupingCompator() {
		super(DateTemperaturePair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateTemperaturePair dateTemperaturePair1 = (DateTemperaturePair) a;
		DateTemperaturePair dateTemperaturePair2 = (DateTemperaturePair) b;
		return dateTemperaturePair1.getYearMonth().compareTo(dateTemperaturePair2.getYearMonth());
	}

}
