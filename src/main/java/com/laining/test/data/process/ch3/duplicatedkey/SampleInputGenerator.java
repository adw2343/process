package com.laining.test.data.process.ch3.duplicatedkey;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Random;
import java.util.StringJoiner;

public class SampleInputGenerator {

	static final int LINE = 300;

	public static void main(String[] args) throws IOException {
		File file = new File("sample_input.txt");
		Random random = new Random();
		try (Writer writer = new BufferedWriter(new FileWriter(file))) {
			for (int i = 1; i <= LINE; i++) {
				StringJoiner stringJoiner = new StringJoiner(",");
				stringJoiner.add(String.valueOf((char) ('A' + random.nextInt(26))));
				stringJoiner.add(String.valueOf(random.nextInt(10)));
				writer.write(stringJoiner.toString() + System.lineSeparator());
			}
		}
	}

}
