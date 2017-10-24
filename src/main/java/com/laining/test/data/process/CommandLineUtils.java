package com.laining.test.data.process;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandLineUtils {

	public static void buildCommandLineOptions(final Options options) {
		Option opt = new Option("f", "file", true, "path for input file");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("n", "topN", true, "top n number that need to cal");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("o", "outPath", true, "out put path");
		opt.setRequired(false);
		options.addOption(opt);

	}

	public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
			CommandLineParser parser) {
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(110);
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption('h')) {
				hf.printHelp(appName, options, true);
				return null;
			}
		} catch (ParseException e) {
			hf.printHelp(appName, options, true);
		}

		return commandLine;
	}

}
