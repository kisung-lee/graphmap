package edu.gatech.cc.vbp.tools;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

@SuppressWarnings("deprecation")
public class RemoteSSH {
	
	public static void runCommandOnMultipleMachines(String slaveFile, String command) {
		List<String> destinations = TextFileManager.readTextFile(slaveFile);
		for(String destination : destinations) {
			String address = "klee@" + destination;
			CommandManager.executeLocalCommand("ssh " + address + " " + "\"" + command + "\"", true, true);
		}
	}
	
	public static void runCommandOnMultipleMachinesWithThreads(String slaveFile, String command) {
		List<String> destinations = TextFileManager.readTextFile(slaveFile);
		List<String> addressList = new ArrayList<String>();
		List<String> commandList = new ArrayList<String>();
		for(String destination : destinations) {
			addressList.add("klee@" + destination);
			commandList.add("\"" + command + "\"");
		}
		
		CommandManager.executeMultipleRemoteCommands(addressList, commandList, true);
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("file name").
				hasArg().
				isRequired().
				withDescription("a slave file name including slave addresses").
				create("slave"));
		options.addOption(OptionBuilder.withArgName("command").
				hasArg().
				isRequired().
				withDescription("command to be executed all servers in the slave file").
				create("command"));
		options.addOption(OptionBuilder.
				withDescription("using threads (recommended)").
				create("thread"));

		try {
			CommandLine line = parser.parse(options, args);
			String slaveFile = line.getOptionValue("slave");
			String command = line.getOptionValue("command");
			if(line.hasOption("thread"))
				runCommandOnMultipleMachinesWithThreads(slaveFile, command);
			else
				runCommandOnMultipleMachines(slaveFile, command);
		} catch (MissingOptionException e) {
			Common.printUsage(options);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}
	}

}
