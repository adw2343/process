package com.laining.test.data.process.ch1.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

	private Text yearMonth = new Text();
	private Text day = new Text();
	private IntWritable temperature = new IntWritable();

	public DateTemperaturePair() {
		super();
	}

	public DateTemperaturePair(Text yearMonth, Text day, IntWritable temperature) {
		super();
		this.yearMonth = yearMonth;
		this.day = day;
		this.temperature = temperature;
	}

	public static DateTemperaturePair read(DataInput in) throws IOException {
		DateTemperaturePair pair = new DateTemperaturePair();
		pair.readFields(in);
		return pair;
	}

	public void readFields(DataInput arg0) throws IOException {
		yearMonth.readFields(arg0);
		day.readFields(arg0);
		temperature.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		yearMonth.write(arg0);
		day.write(arg0);
		temperature.write(arg0);
	}

	public int compareTo(DateTemperaturePair o) {
		int cmp = this.yearMonth.compareTo(o.getYearMonth());
		if (cmp == 0) {
			cmp = temperature.compareTo(o.getTemperature());
		}
		return -1 * cmp;// desc
	}

	public Text getYearMonthDay() {
		return new Text(yearMonth.toString() + day.toString());
	}

	public Text getYearMonth() {
		return yearMonth;
	}

	public void setYearMonth(String yearMonth) {
		this.yearMonth.set(yearMonth);
	}

	public Text getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day.set(day);
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setTemperature(int temp) {
		this.temperature.set(temp);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((temperature == null) ? 0 : temperature.hashCode());
		result = prime * result + ((yearMonth == null) ? 0 : yearMonth.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DateTemperaturePair other = (DateTemperaturePair) obj;
		if (temperature == null) {
			if (other.temperature != null)
				return false;
		} else if (!temperature.equals(other.temperature))
			return false;
		if (yearMonth == null) {
			if (other.yearMonth != null)
				return false;
		} else if (!yearMonth.equals(other.yearMonth))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DateTemperaturePair{yearMonth=");
		builder.append(yearMonth);
		builder.append(", day=");
		builder.append(day);
		builder.append(", temperature=");
		builder.append(temperature);
		builder.append("}");
		return builder.toString();
	}

}
