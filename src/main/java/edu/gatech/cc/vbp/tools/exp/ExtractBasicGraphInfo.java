package edu.gatech.cc.vbp.tools.exp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import edu.gatech.cc.vbp.tools.Common;
import edu.gatech.cc.vbp.tools.TextFileManager;

@SuppressWarnings("deprecation")
public class ExtractBasicGraphInfo {
	
	private static void findMaxVertexID(String inputFileName) throws NumberFormatException, IOException {
		int maxID = -1;
		
		BufferedReader reader = TextFileManager.getBufferedReader(inputFileName);
		String line = null;
		while((line = reader.readLine()) != null) {
			if(line.startsWith("#"))
				continue;
			StringTokenizer st = new StringTokenizer(line);
			if(st.countTokens() != 2)
				continue;
			
			int startVertex = Integer.parseInt(st.nextToken());
			maxID = Math.max(maxID, startVertex);
			int endVertex = Integer.parseInt(st.nextToken());
			maxID = Math.max(maxID, endVertex);
		}
		
		System.out.println("max vertex ID: " + maxID);
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("name").
				hasArg().
				isRequired().
				withDescription("input graph file name").
				create("input"));
		options.addOption(OptionBuilder.withArgName("mode").
				hasArg().
				isRequired().
				withDescription("1: find max vertex ID").
				create("mode"));
		
		
		try {
			CommandLine line = parser.parse(options, args);
			String inputFileName = line.getOptionValue("input");
			int mode = Integer.parseInt(line.getOptionValue("mode"));
			
			if(mode == 1)
				findMaxVertexID(inputFileName);
			else
				Common.printUsage(options);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
