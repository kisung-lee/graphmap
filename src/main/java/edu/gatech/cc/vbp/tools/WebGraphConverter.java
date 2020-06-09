package edu.gatech.cc.vbp.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
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
public class WebGraphConverter {
	
	private static void convertWebGraphToStandfordFormat(String input, String output) {
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			reader = TextFileManager.getBufferedReader(input);
			writer = TextFileManager.getTextFileWriter(output);
			String line = null;
			int count = 0;
			int numVertices = -1;
			int vertexID = 0;
			while((line = reader.readLine()) != null) {
				if(count == 0)	//#vertices
					numVertices = Integer.parseInt(line);
				else {
					StringTokenizer st = new StringTokenizer(line);
					while(st.hasMoreTokens()) {
						int neighbor = Integer.parseInt(st.nextToken());
						writer.write(vertexID+"\t"+neighbor+"\n");
					}
					vertexID++;
					
					if(vertexID % 100000 == 0)
						System.out.println(vertexID + " vertices are processed");
				}
				
				count++;
			}
			
			if(count-1 == numVertices)
				System.out.println(numVertices + " vertices are converted");
			else
				System.out.println("something wrong " + numVertices);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer != null)
					writer.close();
				if(reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
				withDescription("input WebGraph text file").
				create("input"));
		options.addOption(OptionBuilder.withArgName("text file").
				hasArg().
				isRequired().
				withDescription("output file (one edge per line) ").
				create("output"));
		
		try {
			CommandLine line = parser.parse(options, otherArgs);
			String input = line.getOptionValue("input");
			String output = line.getOptionValue("output");
			convertWebGraphToStandfordFormat(input, output);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
