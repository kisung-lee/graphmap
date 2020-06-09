package edu.gatech.cc.vbp.partition;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

import edu.gatech.cc.vbp.graph.UnweightedEdge;
import edu.gatech.cc.vbp.tools.Common;

/**
 * Assumptions: 
 *	1) unweighted (i.e., we assume that the edge weight is 1 for all edges)
 *	2) int type (vertex IDs)
 */
@SuppressWarnings("deprecation")
public class EVBGenerator {
	
	/**
	 * Input: input graph (each line: start_vertex_id end_vertex_id)
	 * Output: output1
	 * 
	 * This Hadoop job does
	 * 1. construct basic Vertex Block (VB)
	 * 2. store border vertices of each VB 
	 */
	public static class VertexBlockGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>  {
		private int mode;	//1 for outgoing, 2 for incoming, 3 for both

		private IntWritable keyInt = new IntWritable();
		private IntWritable valueInt = new IntWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.mode = Integer.parseInt(context.getConfiguration().get("mode"));
			System.out.println("#mode: " + this.mode);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String edgeStr = value.toString().trim();
			if(edgeStr.startsWith("#"))	//comment
				return;
			
			UnweightedEdge edge = new UnweightedEdge(edgeStr);
			if(!edge.isValid())
				return;
			
			if(mode != 2) {	//outgoing or both
				keyInt.set(edge.getStartVertexID());
				valueInt.set(edge.getEndVertexID());
				context.write(keyInt, valueInt);
			}
			
			if(mode != 1) {	//incoming or both
				keyInt.set(edge.getEndVertexID());
				valueInt.set(edge.getStartVertexID());
				context.write(keyInt, valueInt);
			}
		}
	}
	
	public static class VertexBlockGeneratorReduce extends Reducer<IntWritable, IntWritable, Text, Text> {
		
		private Text keyText = new Text();
		private Text valueText = new Text("");

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
		}

		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {	
			for(IntWritable value : values) {	//for each non-anchor vertex
				keyText.set("! " + key.get() + " " + value.get() + " 1 B");	//"! anchor non-anchor distance (B|I)"
				context.write(keyText, valueText);	
 			}
		}
	}
	
	/**
	 * Input: input (original graph), output(i-1) (output of previous extension)
	 * Output: output(i)
	 */
	public static class SingleHopExtensionMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {

		private int mode;	//1 for outgoing, 2 for incoming, 3 for both
		
		private IntWritable keyInt = new IntWritable();
		private Text valueText = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.mode = Integer.parseInt(context.getConfiguration().get("mode"));
			System.out.println("Mode: " + this.mode);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().startsWith("!")) {	//"! anchor non-anchor distance (B|I)"
				StringTokenizer st = new StringTokenizer(value.toString());
				if(st.countTokens() != 5) {	//something wrong
					System.err.println("Wrong input format: " + value.toString());
					return;
				}
				st.nextToken();	//"!"
				String anchor = st.nextToken();	//anchor
				keyInt.set(Integer.parseInt(st.nextToken()));	//non-anchor
				st.nextToken();	//distance
				String type = st.nextToken();
				if(type.equals("B")) {	//do nothing for internal (i.e., non-border) vertices
					valueText.set("! " + anchor);
					context.write(keyInt, valueText);
				}
				
			} else {	//from the input graph
				String edgeStr = value.toString().trim();
				if(edgeStr.startsWith("#"))	//comment
					return;
				
				UnweightedEdge edge = new UnweightedEdge(edgeStr);
				if(!edge.isValid())
					return;
				
				if(mode != 2) {	//outgoing or both
					keyInt.set(edge.getStartVertexID());
					valueText.set(edge.getEndVertexID()+"");
					context.write(keyInt, valueText);
				}
				
				if(mode != 1) {	//incoming or both
					keyInt.set(edge.getEndVertexID());
					valueText.set(edge.getStartVertexID()+"");
					context.write(keyInt, valueText);
				}
			}
		}
	}
	
	public static class SingleHopExtensionReduce extends Reducer<IntWritable, Text, Text, Text> {

		private int currentHop;
		
		private Text keyText = new Text();
		private Text emptyValueText = new Text("");

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			this.currentHop = Integer.parseInt(context.getConfiguration().get("currentHop"));
			System.out.println("currentHop: " + this.currentHop);
		}

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			//key: border vertex
			Set<Integer> anchorList = new HashSet<Integer>(); //a set of anchor vertices having this border vertex
			Set<Integer> newBorderList = new HashSet<Integer>();	//a set of new (possible) border vertices of the border vertex
			
			for(Text value : values) {
				String valueStr = value.toString();
				if(valueStr.startsWith("!")) {	//"! anchor"
					anchorList.add(Integer.parseInt(valueStr.substring(2)));
				} else {	//"newBorder"
					newBorderList.add(Integer.parseInt(valueStr));
				}
			}
			
//			System.out.println(String.format("border: %d, #anchors: %d, #newBorders: %d", key.get(), anchorList.size(), newBorderList.size()));
			
			//we don't need to expand the anchor vertex again => incorrect! (since this border can be a border of several anchors)
//			newBorderList.removeAll(anchorList);
			
			if(anchorList.size() == 0 || newBorderList.size() == 0)
				return;	//do nothing

			for(int anchor : anchorList) {
				//the border vertex is now a non-border vertex since the vertex is being expanded
				keyText.set("! " + anchor + " " + key.get() + " " + (currentHop-1) + " I");
				context.write(keyText, emptyValueText);
				
				for(int newBorder : newBorderList) {
					if(anchor != newBorder) {
						keyText.set("! " + anchor + " " + newBorder + " " + currentHop + " B");
						context.write(keyText, emptyValueText);
					}
				}
			}
		}
	}
	
	/**
	 * input: output1, ..., output(k)
	 * output: output(k+1)
	 */
	public static class FinalMergeMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {

		private IntWritable keyInt = new IntWritable();
		private Text valueText = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//"! anchor non-anchor distance (B|I)"
			String valueStr = value.toString().substring(2);	//remove "!"
			int indexOfSpace = valueStr.indexOf(" ");
			keyInt.set(Integer.parseInt(valueStr.substring(0, indexOfSpace)));	//anchor
			valueText.set(valueStr.substring(indexOfSpace+1));	//"non-anchor distance (B|I)"
			context.write(keyInt, valueText);
		}
	}
	
	protected static class NonAnchorInfo {
		protected int distance;
		protected boolean isBorder = true;
		
		public NonAnchorInfo(int distance, boolean isBorder) {
			super();
			this.distance = distance;
			this.isBorder = isBorder;
		}
	}
	
	public static class FinalMergeReduce extends Reducer<IntWritable, Text, Text, Text> {

		private Text keyText = new Text();
		private Text emptyValueText = new Text("");

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			//key: anchor vertex
			Map<Integer, NonAnchorInfo> nonAnchorVertices = new HashMap<Integer, EVBGenerator.NonAnchorInfo>();
			
			for(Text value : values) {	//"non-anchor distance (B|I)"
				StringTokenizer st = new StringTokenizer(value.toString());
				if(st.countTokens() != 3)	//something wrong
					continue;
				int nonAnchor = Integer.parseInt(st.nextToken());
				int distance = Integer.parseInt(st.nextToken());
				boolean isBorder = st.nextToken().equals("B") ? true : false;
				
				NonAnchorInfo nonAnchorInfo = nonAnchorVertices.get(nonAnchor);
				if(nonAnchorInfo == null) {
					nonAnchorVertices.put(nonAnchor, new NonAnchorInfo(distance, isBorder));
				} else {
					if(distance < nonAnchorInfo.distance)
						nonAnchorInfo.distance = distance;
					if(!isBorder)
						nonAnchorInfo.isBorder = false;
				}
			}
			
			String evb = key.get()+"";	//anchor vertex
			for(int nonAnchorVertexID : nonAnchorVertices.keySet()) {
				NonAnchorInfo nonAnchorInfo = nonAnchorVertices.get(nonAnchorVertexID);
				evb += " " + nonAnchorVertexID + " " + nonAnchorInfo.distance + " " + (nonAnchorInfo.isBorder ? "B" : "I");	//"non-anchor distance (B|I)"
			}
			keyText.set(evb);
			context.write(keyText, emptyValueText);
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
				isRequired().
				withDescription("number of reducers").
				create("numReducers"));
		options.addOption(OptionBuilder.withArgName("#hops").
				hasArg().
				isRequired().
				withDescription("number of hops (> 0)").
				create("numHops"));
		options.addOption(OptionBuilder.withArgName("mode").
				hasArg().
				isRequired().
				withDescription("1 for outgoing, 2 for incoming, 3 for both").
				create("mode"));
		
		try {
			Calendar start = Calendar.getInstance();
			
			CommandLine line = parser.parse(options, otherArgs);
			String inputDirectory = line.getOptionValue("input");
			String outputDirectory = line.getOptionValue("output");
			
			int numReducers = Integer.parseInt(line.getOptionValue("numReducers"));
			int numHops = Integer.parseInt(line.getOptionValue("numHops"));
			int mode = Integer.parseInt(line.getOptionValue("mode"));
			
			if(numHops < 1 || numReducers < 1)
				Common.printUsage(options);
			if(mode < 1 || mode > 3)
				Common.printUsage(options);
			
			{
				Configuration confVBGenerator = new Configuration();
				confVBGenerator.set("mode", mode+"");
				
				Job jobVBGenerator = new Job(confVBGenerator);
				jobVBGenerator.setJobName("VB Construction : mode " + mode);
				FileInputFormat.addInputPath(jobVBGenerator, new Path(inputDirectory));	//input
				FileOutputFormat.setOutputPath(jobVBGenerator, new Path(outputDirectory+1));	//output1
				
				jobVBGenerator.setJarByClass(EVBGenerator.class);
				jobVBGenerator.setMapperClass(VertexBlockGeneratorMapper.class);
				jobVBGenerator.setReducerClass(VertexBlockGeneratorReduce.class);
				jobVBGenerator.setNumReduceTasks(numReducers);

				jobVBGenerator.setInputFormatClass(TextInputFormat.class);
				jobVBGenerator.setOutputFormatClass(TextOutputFormat.class);
				jobVBGenerator.setMapOutputKeyClass(IntWritable.class);
				jobVBGenerator.setMapOutputValueClass(IntWritable.class);
				jobVBGenerator.setOutputKeyClass(Text.class);
				jobVBGenerator.setOutputValueClass(Text.class);

				jobVBGenerator.waitForCompletion(true);
			}
			
			{
				for(int currentHop = 2; currentHop <= numHops; currentHop++) {
					Configuration confSingleHop = new Configuration();
					confSingleHop.set("mode", mode+"");
					confSingleHop.set("currentHop", currentHop+"");
					
					Job jobSingleHop = new Job(confSingleHop);
					jobSingleHop.setJobName(String.format("%d Hop Expansion (Mode: %d)", currentHop, mode));
					
					FileInputFormat.addInputPath(jobSingleHop, new Path(outputDirectory+(currentHop-1)));	//output(currentHop-1)
					FileInputFormat.addInputPath(jobSingleHop, new Path(inputDirectory));	//input
					FileOutputFormat.setOutputPath(jobSingleHop, new Path(outputDirectory+currentHop));	//output(currentHop)
					
					jobSingleHop.setJarByClass(EVBGenerator.class);
					jobSingleHop.setMapperClass(SingleHopExtensionMapper.class);
					jobSingleHop.setReducerClass(SingleHopExtensionReduce.class);
					jobSingleHop.setNumReduceTasks(numReducers);
					
					jobSingleHop.setInputFormatClass(TextInputFormat.class);
					jobSingleHop.setOutputFormatClass(TextOutputFormat.class);
					jobSingleHop.setMapOutputKeyClass(IntWritable.class);
					jobSingleHop.setMapOutputValueClass(Text.class);
					jobSingleHop.setOutputKeyClass(Text.class);
					jobSingleHop.setOutputValueClass(Text.class);

					jobSingleHop.waitForCompletion(true);
				}
			}
			
			{
				Configuration confResultMerge = new Configuration();
				
				Job jobResultMerge = new Job(confResultMerge);
				jobResultMerge.setJobName("Storing EVBs");
				
				for(int i=1; i <=numHops; i++)
					FileInputFormat.addInputPath(jobResultMerge, new Path(outputDirectory+i));

				FileOutputFormat.setOutputPath(jobResultMerge, new Path(outputDirectory+(numHops+1)));	//output(numHops+1)

				jobResultMerge.setJarByClass(EVBGenerator.class);
				jobResultMerge.setMapperClass(FinalMergeMapper.class);
				jobResultMerge.setReducerClass(FinalMergeReduce.class);
				jobResultMerge.setNumReduceTasks(numReducers);

				jobResultMerge.setInputFormatClass(TextInputFormat.class);
				jobResultMerge.setOutputFormatClass(TextOutputFormat.class);
				jobResultMerge.setMapOutputKeyClass(IntWritable.class);
				jobResultMerge.setMapOutputValueClass(Text.class);
				jobResultMerge.setOutputKeyClass(Text.class);
				jobResultMerge.setOutputValueClass(Text.class);

				jobResultMerge.waitForCompletion(true);
			}
			
			System.out.println(String.format("Total processing time: %d seconds", (Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis())/1000));
			
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}
	}

}
