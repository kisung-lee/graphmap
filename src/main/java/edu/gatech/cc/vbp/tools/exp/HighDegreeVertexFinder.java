package edu.gatech.cc.vbp.tools.exp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import edu.gatech.cc.vbp.tools.Common;
import edu.gatech.cc.vbp.tools.TextFileManager;

@SuppressWarnings("deprecation")
public class HighDegreeVertexFinder {
	
	public static void findOutDegreeHDVInMemory(String inputFileName, int numOutput) throws IOException {
		Map<Integer, Integer> counters = new HashMap<Integer, Integer>();	//vertex ID => # out-edges
		
		BufferedReader reader = TextFileManager.getBufferedReader(inputFileName);
		String line = null;
		while((line = reader.readLine()) != null) {
			if(line.startsWith("#"))
				continue;
			StringTokenizer st = new StringTokenizer(line);
			if(st.countTokens() != 2)
				continue;
			
			int startVertex = Integer.parseInt(st.nextToken());
//			int endVertex = Integer.parseInt(st.nextToken());
			
			Integer counter = counters.get(startVertex);
			if(counter == null)
				counter = 0;
			counters.put(startVertex, counter+1);
		}
		
		//merge by #out-edges
		SortedMap<Integer, List<Integer>> histogram = new TreeMap<Integer, List<Integer>>();	//#out-edges => vertex ID list
		for(int vertexID : counters.keySet()) {
			int numOutEdges = counters.get(vertexID);
			List<Integer> vertexList = histogram.get(numOutEdges);
			if(vertexList == null) {
				vertexList = new ArrayList<Integer>();
				histogram.put(numOutEdges, vertexList);
			}
			vertexList.add(vertexID);
		}
		
		//print
		int countPrinted = 0;
		while(histogram.size() > 0) {
			int numOutEdges = histogram.lastKey();
			List<Integer> vertexList = histogram.get(numOutEdges);
			for(int vertexID : vertexList) {
				System.out.println(vertexID + " : " + numOutEdges + " out-edges");
				countPrinted++;
				if(countPrinted >= numOutput)
					break;
			}
			if(countPrinted >= numOutput)
				break;
			histogram.remove(numOutEdges);
		}
	}
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException {
		
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("name").
				hasArg().
				isRequired().
				withDescription("input graph file name").
				create("input"));
		options.addOption(OptionBuilder.withArgName("num").
				hasArg().
				isRequired().
				withDescription("# high degree vertices").
				create("num"));
		
		
		try {
			CommandLine line = parser.parse(options, args);
			String inputFileName = line.getOptionValue("input");
			int numOutput = Integer.parseInt(line.getOptionValue("num"));
			
			findOutDegreeHDVInMemory(inputFileName, numOutput);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}
}
