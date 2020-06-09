package edu.gatech.cc.vbp.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class TwoResultsComparor {
	
	private static void compareTwoResults(String firstResultFile, String secondResultFile) {
		Map<String, String> firstResultMap = new HashMap<String, String>();
		
		try {
			
			BufferedReader firstReader = TextFileManager.getBufferedReader(firstResultFile);
			String line = null;
			while((line = firstReader.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);
				if(st.countTokens() != 2) {
					System.out.println("something wrong (first file): " + line);
					continue;
				}
				firstResultMap.put(st.nextToken(), st.nextToken());
			}
			int firstNumVertices = firstResultMap.size();
			int secondNumVertices = 0;
			int countNoMatching = 0;
			
			BufferedReader secondReader = TextFileManager.getBufferedReader(secondResultFile);
			while((line = secondReader.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);
				if(st.countTokens() != 2) {
					System.out.println("something wrong (second file): " + line);
					continue;
				}
				secondNumVertices++;
				String vertex = st.nextToken();
				String value = st.nextToken();
				
				String firstValue = firstResultMap.get(vertex);
				if(firstValue == null) {
					System.out.println("first results don't have vertex " + vertex);
					continue;
				}
				if(!value.equals(firstValue)) {
					countNoMatching++;
					System.out.println("No matching for vertex " + vertex + "! : [" + firstValue + "] vs [" + value + "]");
					continue;
				}
			}
			
			System.out.println("#vertices (first): " + firstNumVertices);
			System.out.println("#vertices (second): " + secondNumVertices);
			if(firstNumVertices == secondNumVertices && countNoMatching == 0)
				System.out.println("Perfect matching!");
			else
				System.out.println("# no matching values: " + countNoMatching);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void compareTwoResultsConsideringInfinity(String firstResultFile, String secondResultFile) {
		Map<String, String> firstResultMap = new HashMap<String, String>();
		
		try {
			
			BufferedReader firstReader = TextFileManager.getBufferedReader(firstResultFile);
			String line = null;
			while((line = firstReader.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);
				if(st.countTokens() != 2) {
					System.out.println("something wrong (first file): " + line);
					continue;
				}
				firstResultMap.put(st.nextToken(), st.nextToken());
			}
			int firstNumVertices = firstResultMap.size();
			int secondNumVertices = 0;
			int countNoMatching = 0;
			int countInfinity = 0;
			
			BufferedReader secondReader = TextFileManager.getBufferedReader(secondResultFile);
			while((line = secondReader.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);
				if(st.countTokens() != 2) {
					System.out.println("something wrong (second file): " + line);
					continue;
				}
				secondNumVertices++;
				String vertex = st.nextToken();
				String value = st.nextToken();
				if(value.equals(Integer.MAX_VALUE+""))
					countInfinity++;
				
				String firstValue = firstResultMap.get(vertex);
				if(firstValue == null) {
//					System.out.println("first results don't have vertex " + vertex);
					continue;
				}
				if(!value.equals(firstValue)) {
					countNoMatching++;
					System.out.println("No matching for vertex " + vertex + "! : [" + firstValue + "] vs [" + value + "]");
					continue;
				}
			}
			
			System.out.println("#vertices (first): " + firstNumVertices);
			System.out.println("#vertices (second): " + secondNumVertices);
			System.out.println("#vertices having infinity (second): " + countInfinity);
			if(firstNumVertices == secondNumVertices-countInfinity && countNoMatching == 0)
				System.out.println("Perfect matching!");
			else
				System.out.println("# no matching values: " + countNoMatching);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("text file").
				hasArg().
				isRequired().
				withDescription("first result file (text)").
				create("first"));
		options.addOption(OptionBuilder.withArgName("text file").
				hasArg().
				isRequired().
				withDescription("second result file (text)").
				create("second"));
		options.addOption(OptionBuilder.withArgName("mode").
				hasArg().
				withDescription("1: default, 2: consider infinity").
				create("mode"));
		
		try {
			CommandLine line = parser.parse(options, otherArgs);
			String firstResultFile = line.getOptionValue("first");
			String secondResultFile = line.getOptionValue("second");
			int mode = 1;
			if(line.hasOption("mode"))
				mode = Integer.parseInt(line.getOptionValue("mode"));
			if(mode == 1)
				compareTwoResults(firstResultFile, secondResultFile);
			else
				compareTwoResultsConsideringInfinity(firstResultFile, secondResultFile);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		

	}

}
