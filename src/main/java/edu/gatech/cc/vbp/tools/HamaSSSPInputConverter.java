package edu.gatech.cc.vbp.tools;

import java.io.IOException;

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

import edu.gatech.cc.vbp.graph.UnweightedEdge;

/**
 * @deprecated
 * @author Greg
 *
 */
public class HamaSSSPInputConverter {
	
	public static class HamaSSSPInputConverterMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable keyInt = new IntWritable();
		private IntWritable valueInt = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//"start_vertex end_vertex"
			String edgeStr = value.toString().trim();
			if(edgeStr.startsWith("#"))	//comment
				return;
			
			UnweightedEdge edge = new UnweightedEdge(edgeStr);
			if(!edge.isValid())
				return;
			
			keyInt.set(edge.getStartVertexID());
			valueInt.set(edge.getEndVertexID());
			context.write(keyInt, valueInt);
		}
	}

	public static class HamaSSSPInputConverterReduce extends Reducer<IntWritable, IntWritable, Text, Text> {

		private Text keyText = new Text();
		private Text valueText = new Text("");

		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {	
			String str = key.get()+"";
			for(IntWritable value : values) {	
				str += "\t" + value.get() + ":1";	
			}
			keyText.set(str);
			context.write(keyText, valueText);
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
			
			Configuration confHamaSSSPInputConverter = new Configuration();
			Job jobHamaSSSPInputConverter = new Job(confHamaSSSPInputConverter);
			jobHamaSSSPInputConverter.setJobName("Converting the graph into Hama SSSP format");
			FileInputFormat.addInputPath(jobHamaSSSPInputConverter, new Path(inputDirectory));	
			FileOutputFormat.setOutputPath(jobHamaSSSPInputConverter, new Path(outputDirectory));	

			jobHamaSSSPInputConverter.setJarByClass(HamaSSSPInputConverter.class);
			jobHamaSSSPInputConverter.setMapperClass(HamaSSSPInputConverterMapper.class);
			jobHamaSSSPInputConverter.setReducerClass(HamaSSSPInputConverterReduce.class);
			jobHamaSSSPInputConverter.setNumReduceTasks(10);

			jobHamaSSSPInputConverter.setInputFormatClass(TextInputFormat.class);
			jobHamaSSSPInputConverter.setOutputFormatClass(TextOutputFormat.class);
			jobHamaSSSPInputConverter.setMapOutputKeyClass(IntWritable.class);
			jobHamaSSSPInputConverter.setMapOutputValueClass(IntWritable.class);
			jobHamaSSSPInputConverter.setOutputKeyClass(Text.class);
			jobHamaSSSPInputConverter.setOutputValueClass(Text.class);

			jobHamaSSSPInputConverter.waitForCompletion(true);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}

}
