package edu.gatech.cc.vbp.tools;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Common {
	public static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("available options as follow:", options );
		System.exit(1);
	}
}
