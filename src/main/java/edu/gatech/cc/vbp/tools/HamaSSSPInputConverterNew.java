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

@SuppressWarnings("deprecation")
public class HamaSSSPInputConverterNew {
	
	public static class HamaSSSPInputConverterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
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

	public static class HamaSSSPInputConverterReduce extends Reducer<IntWritable, Text, Text, Text> {

		private Text keyText = new Text();
		private Text valueText = new Text("");

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			/*String str = key.get()+"";
			for(Text value : values) {
				if(value.toString().startsWith("O")) {	//store only outgoing edges					
					str += "\t" + value.toString().substring(2).trim() + ":1";	//remove "O"	
				}
			}
			keyText.set(str);
			context.write(keyText, valueText);*/
			
			StringBuilder sb = new StringBuilder(key.get()+"");
			for(Text value : values) {
				if(value.toString().startsWith("O")) {	//store only outgoing edges					
					sb.append("\t" + value.toString().substring(2).trim() + ":1");//remove "O"	
				}
			}
			keyText.set(sb.toString());
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
		options.addOption(OptionBuilder.withArgName("#reducers").
				hasArg().
				withDescription("number of reducers").
				create("numReducers"));
		
		try {
			Calendar start = Calendar.getInstance();
			
			CommandLine line = parser.parse(options, otherArgs);
			String inputDirectory = line.getOptionValue("input");
			String outputDirectory = line.getOptionValue("output");
			
			Configuration confHamaSSSPInputConverter = new Configuration();
			Job jobHamaSSSPInputConverter = new Job(confHamaSSSPInputConverter);
			jobHamaSSSPInputConverter.setJobName("Converting the graph into Hama SSSP format");
			FileInputFormat.addInputPath(jobHamaSSSPInputConverter, new Path(inputDirectory));	
			FileOutputFormat.setOutputPath(jobHamaSSSPInputConverter, new Path(outputDirectory));	

			jobHamaSSSPInputConverter.setJarByClass(HamaSSSPInputConverterNew.class);
			jobHamaSSSPInputConverter.setMapperClass(HamaSSSPInputConverterMapper.class);
			jobHamaSSSPInputConverter.setReducerClass(HamaSSSPInputConverterReduce.class);
			if(line.hasOption("numReducers"))
				jobHamaSSSPInputConverter.setNumReduceTasks(Integer.parseInt(line.getOptionValue("numReducers")));
			else
				jobHamaSSSPInputConverter.setNumReduceTasks(10);

			jobHamaSSSPInputConverter.setInputFormatClass(TextInputFormat.class);
			jobHamaSSSPInputConverter.setOutputFormatClass(TextOutputFormat.class);
			jobHamaSSSPInputConverter.setMapOutputKeyClass(IntWritable.class);
			jobHamaSSSPInputConverter.setMapOutputValueClass(Text.class);
			jobHamaSSSPInputConverter.setOutputKeyClass(Text.class);
			jobHamaSSSPInputConverter.setOutputValueClass(Text.class);

			jobHamaSSSPInputConverter.waitForCompletion(true);
			
			System.out.println(String.format("Total processing time: %d seconds", (Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis())/1000));
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}

}
