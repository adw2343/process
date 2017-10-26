package com.laining.test.data.process.ch4;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Random;
import java.util.StringJoiner;

public class SampleInputGenerator {
	private static final String[] LOCATIONS = new String[] { "UT", "GT", "CA", "GD", "HK", "UH", "PH", "LA", "MN", "BG",
			"BF" };
	private static final int USER_NUMBER = 100;
	private static final int PRODUCT_NUMBER = 20;

	private static final int TRANSACTION_NUMBER = 500;
	private static final String USER_INPUT_FILE_NAME = "user_sample_input.txt";
	private static final String TRANSACTION_INPUT_FILE_NAME = "transaction_sample_input.txt";

	public static void main(String[] args) throws IOException {
		File userFile = new File(USER_INPUT_FILE_NAME);
		File transactionFile = new File(TRANSACTION_INPUT_FILE_NAME);

		Random random = new Random();
		try (Writer writer = new BufferedWriter(new FileWriter(userFile))) {
			for (int i = 1; i <= USER_NUMBER; i++) {
				StringJoiner stringJoiner = new StringJoiner(",");
				stringJoiner.add("u" + i);
				stringJoiner.add(LOCATIONS[random.nextInt(LOCATIONS.length - 1)]);
				writer.write(stringJoiner.toString() + System.lineSeparator());
			}
		}

		try (Writer writer = new BufferedWriter(new FileWriter(transactionFile))) {
			for (int i = 1; i <= TRANSACTION_NUMBER; i++) {
				StringJoiner stringJoiner = new StringJoiner(",");
				stringJoiner.add("t" + i);
				stringJoiner.add("u" + (random.nextInt(99) + 1));
				stringJoiner.add("p" + (random.nextInt(PRODUCT_NUMBER) + 1));
				writer.write(stringJoiner.toString() + System.lineSeparator());
			}
		}
	}

}
