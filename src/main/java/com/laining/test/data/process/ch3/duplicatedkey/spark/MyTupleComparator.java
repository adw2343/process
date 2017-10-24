package com.laining.test.data.process.ch3.duplicatedkey.spark;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

	private static final long serialVersionUID = -5537506951025872653L;

	public static final Comparator<Tuple2<String, Integer>> INSTANCE = new MyTupleComparator();

	@Override
	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
		return -o1._2().compareTo(o2._2());
	}

}
