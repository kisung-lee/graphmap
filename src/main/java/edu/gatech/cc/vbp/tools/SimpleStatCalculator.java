package edu.gatech.cc.vbp.tools;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class SimpleStatCalculator {
	
	public static class SimpleStatCalculatorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable keyInt = new IntWritable(1);
		private Text valueText = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//"anchor non-anchor1 distance1 (B|I) non-anchor2 distance2 (B|I) ..."
			String valueStr = value.toString();
			int indexOfSpace = valueStr.indexOf(" ");
			int anchor;
			int numEdges = 0;
			if(indexOfSpace == -1)	//EVB having only anchor
				anchor = Integer.parseInt(valueStr.trim());
			else {
				anchor = Integer.parseInt(valueStr.substring(0, indexOfSpace));
				StringTokenizer st = new StringTokenizer(valueStr.substring(indexOfSpace+1));
				while(st.hasMoreTokens()) {
					st.nextToken();	//non-anchor
					st.nextToken(); //distance
					st.nextToken(); //B or I
					numEdges++;
				}
			}
			valueText.set(anchor + " " + numEdges);	//"anchor #edges"
			context.write(keyInt, valueText);
		}
	}
	
	public static class SimpleStatCalculatorReduce extends Reducer<IntWritable, Text, Text, Text> {

		private Text valueText = new Text("");

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			int numAnchorVertices = 0;
			int anchorWithMax = -1;
			int anchorWithMin = -1;
			int minEdges = Integer.MAX_VALUE;
			int maxEdges = Integer.MIN_VALUE;
			int totalEdges = 0;
			
			for(Text value : values) {	//"anchor #edges"
				numAnchorVertices++;
				StringTokenizer st = new StringTokenizer(value.toString());
				int anchor = Integer.parseInt(st.nextToken());
				int numEdges = Integer.parseInt(st.nextToken());
				if(numEdges < minEdges) {
					minEdges = numEdges;
					anchorWithMin = anchor;
				}
				if(numEdges > maxEdges) {
					maxEdges = numEdges;
					anchorWithMax = anchor;
				}
				totalEdges += numEdges;
			}
			context.write(new Text("# anchor vertices: " + numAnchorVertices), valueText);
			context.write(new Text("# total edges (in EVBs): " + totalEdges), valueText);
			context.write(new Text("# avg. edges per EVB: " + totalEdges/(double)numAnchorVertices), valueText);
			context.write(new Text("# min edges: " + minEdges + " of anchor vertex " + anchorWithMin), valueText);
			context.write(new Text("# max edges: " + maxEdges + " of anchor vertex " + anchorWithMax), valueText);
		}
	}
	

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("dir").
				hasArg().
				isRequired().
				withDescription("input directory (HDFS)").
				create("input"));
		options.addOption(OptionBuilder.withArgName("dir").
				hasArg().
				isRequired().
				withDescription("output directory (HDFS)").
				create("output"));
		
		try {
			CommandLine line = parser.parse(options, otherArgs);
			String inputDirectory = line.getOptionValue("input");
			String outputDirectory = line.getOptionValue("output");
			
			Configuration confSimpleStatCalculator = new Configuration();
			Job jobSimpleStatCalculator = new Job(confSimpleStatCalculator);
			jobSimpleStatCalculator.setJobName("Simple Stat Calculator");
			FileInputFormat.addInputPath(jobSimpleStatCalculator, new Path(inputDirectory));	
			FileOutputFormat.setOutputPath(jobSimpleStatCalculator, new Path(outputDirectory));	

			jobSimpleStatCalculator.setJarByClass(SimpleStatCalculator.class);
			jobSimpleStatCalculator.setMapperClass(SimpleStatCalculatorMapper.class);
			jobSimpleStatCalculator.setReducerClass(SimpleStatCalculatorReduce.class);
			jobSimpleStatCalculator.setNumReduceTasks(1);

			jobSimpleStatCalculator.setInputFormatClass(TextInputFormat.class);
			jobSimpleStatCalculator.setOutputFormatClass(TextOutputFormat.class);
			jobSimpleStatCalculator.setMapOutputKeyClass(IntWritable.class);
			jobSimpleStatCalculator.setMapOutputValueClass(Text.class);
			jobSimpleStatCalculator.setOutputKeyClass(Text.class);
			jobSimpleStatCalculator.setOutputValueClass(Text.class);

			jobSimpleStatCalculator.waitForCompletion(true);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}
		
	}

}
