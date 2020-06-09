package edu.gatech.cc.vbp.tools;

import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class EmptyHadoopJob {
	
	public static class EmptyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//do nothing
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
			Calendar start = Calendar.getInstance();
			
			CommandLine line = parser.parse(options, otherArgs);
			String inputDirectory = line.getOptionValue("input");
			String outputDirectory = line.getOptionValue("output");
			
			Configuration confEmptyJob = new Configuration();
			Job jobEmptyJob = new Job(confEmptyJob);
			jobEmptyJob.setJobName("Empty Hadoop Job");
			FileInputFormat.addInputPath(jobEmptyJob, new Path(inputDirectory));	
			FileOutputFormat.setOutputPath(jobEmptyJob, new Path(outputDirectory));	

			jobEmptyJob.setJarByClass(EmptyHadoopJob.class);
			jobEmptyJob.setMapperClass(EmptyMapper.class);
//			jobEmptyJob.setReducerClass(SimpleStatCalculatorReduce.class);
			jobEmptyJob.setNumReduceTasks(0);

			jobEmptyJob.setInputFormatClass(TextInputFormat.class);
			jobEmptyJob.setOutputFormatClass(TextOutputFormat.class);
			jobEmptyJob.setMapOutputKeyClass(Text.class);
			jobEmptyJob.setMapOutputValueClass(Text.class);
			jobEmptyJob.setOutputKeyClass(Text.class);
			jobEmptyJob.setOutputValueClass(Text.class);

			jobEmptyJob.waitForCompletion(true);
			
			System.out.println(String.format("Total processing time: %d seconds", (Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis())/1000));
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

}
