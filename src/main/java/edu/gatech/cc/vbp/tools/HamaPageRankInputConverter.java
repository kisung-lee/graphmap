package edu.gatech.cc.vbp.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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

import edu.gatech.cc.vbp.graph.UnweightedEdge;


@Deprecated
public class HamaPageRankInputConverter {
	
	public static class HamaPageRankInputConverterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable keyInt = new IntWritable();
		private Text valueText = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//"start_vertex end_vertex"
			String edgeStr = value.toString().trim();
			if(edgeStr.startsWith("#"))	//comment
				return;
			
			UnweightedEdge edge = new UnweightedEdge(edgeStr);
			if(!edge.isValid())
				return;
			
			//outgoing
			keyInt.set(edge.getStartVertexID());
			valueText.set("O " + edge.getEndVertexID());
			context.write(keyInt, valueText);
			
			//incoming
			keyInt.set(edge.getEndVertexID());
			valueText.set("I " + edge.getStartVertexID());
			context.write(keyInt, valueText);
		}
	}

	static class TextArrayWritable extends ArrayWritable {

		public TextArrayWritable() {
			super(Text.class);
		}

	}
	
	public static class HamaPageRankInputConverterReduce extends Reducer<IntWritable, Text, Text, TextArrayWritable> {

		private Text keyText = new Text();

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			keyText.set(key.get()+"");
			TextArrayWritable textArrayWritable = new TextArrayWritable();
			
			List<Text> neighborList = new ArrayList<Text>();
			
			for(Text value : values) {
				if(value.toString().startsWith("O")) {	//store only outgoing edges					
					neighborList.add(new Text(value.toString().substring(2).trim()));	//remove "O"
				}
			}
			
			Text[] arr = new Text[neighborList.size()];
			textArrayWritable.set(neighborList.toArray(arr));
			
			context.write(keyText, textArrayWritable);
		}
	}

	/**
	 * @param args
	 * @throws IOException 
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
			
			Configuration confHamaPageRankInputConverter = new Configuration();
			Job jobHamaPageRankInputConverter = new Job(confHamaPageRankInputConverter);
			jobHamaPageRankInputConverter.setJobName("Converting the graph into Hama PageRank format");
			FileInputFormat.addInputPath(jobHamaPageRankInputConverter, new Path(inputDirectory));	
			FileOutputFormat.setOutputPath(jobHamaPageRankInputConverter, new Path(outputDirectory));	

			jobHamaPageRankInputConverter.setJarByClass(HamaPageRankInputConverter.class);
			jobHamaPageRankInputConverter.setMapperClass(HamaPageRankInputConverterMapper.class);
			jobHamaPageRankInputConverter.setReducerClass(HamaPageRankInputConverterReduce.class);
			jobHamaPageRankInputConverter.setNumReduceTasks(10);

			jobHamaPageRankInputConverter.setInputFormatClass(TextInputFormat.class);
			jobHamaPageRankInputConverter.setOutputFormatClass(TextOutputFormat.class);
			jobHamaPageRankInputConverter.setMapOutputKeyClass(IntWritable.class);
			jobHamaPageRankInputConverter.setMapOutputValueClass(Text.class);
			jobHamaPageRankInputConverter.setOutputKeyClass(Text.class);
			jobHamaPageRankInputConverter.setOutputValueClass(TextArrayWritable.class);

			jobHamaPageRankInputConverter.waitForCompletion(true);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}

}
