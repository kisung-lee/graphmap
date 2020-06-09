package edu.gatech.cc.vbp.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
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
public class HBaseGraphStoreJob {
	
	/**
	 * Input: input graph (each line: start_vertex_id end_vertex_id)
	 * Output: output1
	 * 
	 * This Hadoop job does
	 * 1. 
	 */
	public static class HBaseStoreMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {

		private IntWritable keyInt = new IntWritable();
		private Text valueText = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String edgeStr = value.toString().trim();
			if(edgeStr.startsWith("#"))	//comment
				return;
			
			UnweightedEdge edge = new UnweightedEdge(edgeStr);
			if(!edge.isValid())
				return;
			
			keyInt.set(edge.getStartVertexID());
			valueText.set("O " + edge.getEndVertexID());
			context.write(keyInt, valueText);
			
			keyInt.set(edge.getEndVertexID());
			valueText.set("I " + edge.getStartVertexID());
			context.write(keyInt, valueText);
		}
	}
	
	public static class HBaseStoreReduce extends Reducer<IntWritable, Text, Text, Text> {
		private int mode;	//1 for outgoing, 2 for incoming, 3 for both
		private String tableName;	//HBase table name
		private HTable table; 
		private int numHashPartitions;
		

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.mode = Integer.parseInt(context.getConfiguration().get("mode"));
			System.out.println("#mode: " + this.mode);
			this.tableName = context.getConfiguration().get("table");
			System.out.println("HBase table: " + this.tableName);
			this.numHashPartitions = Integer.parseInt(context.getConfiguration().get("numHashPartitions"));
			System.out.println("Stroing hash partitions => #partitions: " + this.numHashPartitions);
			
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, this.tableName);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.close();
			super.cleanup(context);
		}

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			Set<Integer> outSet = new HashSet<Integer>();
			Set<Integer> inSet = new HashSet<Integer>();
			
			for(Text value : values) {	//for each non-anchor vertex
				if(value.toString().startsWith("O")) {
					outSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else if(value.toString().startsWith("I")) {
					inSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else {
					System.out.println("Something wrong: " + value.toString());
					continue;
				}
			}
			
			String outString = "";
			for(int out : outSet)
				outString += out + " ";
			String inString = "";
			for(int in : inSet)
				inString += in + " ";
			
//			Configuration config = HBaseConfiguration.create();
//			HTable table = new HTable(config, this.tableName);
			
			Put put = new Put(Bytes.toBytes(key.get()));
			if(mode == 1 || mode == 3) {	//outgoing or both
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("out"), Bytes.toBytes(outString));
			} 
			if(mode == 2 || mode == 3) {	//incoming or both
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("in"), Bytes.toBytes(inString));
			} 
			table.put(put);
			
			if(numHashPartitions > 0) {	//store a vertex list for each hash value (partition)
				int hashValue = key.get() % numHashPartitions;
				Put partitionPut = new Put(Bytes.toBytes("hash"+hashValue)); //e.g., hash0, hash3
				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), null);	//is null correct?
//				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), Bytes.toBytes(""));	
//				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), new byte[]{});	
//				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), Bytes.toBytes(true));
				table.put(partitionPut);
			}
			
//			table.close();
		}
	}
	
	public static class HBaseStoreUsingSeparateColumnsReduce extends Reducer<IntWritable, Text, Text, Text> {
		private int mode;	//1 for outgoing, 2 for incoming, 3 for both
		private String tableName;	//HBase table name
		private HTable table; 
		private int numHashPartitions;
		

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.mode = Integer.parseInt(context.getConfiguration().get("mode"));
			System.out.println("#mode: " + this.mode);
			this.tableName = context.getConfiguration().get("table");
			System.out.println("HBase table: " + this.tableName);
			this.numHashPartitions = Integer.parseInt(context.getConfiguration().get("numHashPartitions"));
			System.out.println("Stroing hash partitions => #partitions: " + this.numHashPartitions);
			
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, this.tableName);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.close();
			super.cleanup(context);
		}

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			Set<Integer> outSet = new HashSet<Integer>();
			Set<Integer> inSet = new HashSet<Integer>();
			
			for(Text value : values) {	//for each non-anchor vertex
				if(value.toString().startsWith("O")) {
					outSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else if(value.toString().startsWith("I")) {
					inSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else {
					System.out.println("Something wrong: " + value.toString());
					continue;
				}
			}
			
			{
				int countNeighbor = 0;
				Put put = new Put(Bytes.toBytes(key.get()));
				if(mode == 1 || mode == 3) {	//outgoing or both
					for(Integer outVertex : outSet) {
						put.add(Bytes.toBytes("cf"), Bytes.toBytes(outVertex), null);
						countNeighbor++;
					}
				} 
				if(mode == 2 || mode == 3) {	//incoming or both
					//TODO need to use a different column family
					for(Integer inVertex : inSet) {
						put.add(Bytes.toBytes("cf"), Bytes.toBytes(inVertex), null);
						countNeighbor++;
					}
				} 
				if(countNeighbor == 0)
					put.add(Bytes.toBytes("cf"), Bytes.toBytes(-1), null);	//to indicate this vertex has no neighbor
				table.put(put);
			}
			
			if(numHashPartitions > 0) {	//store a vertex list for each hash value (partition)
				int hashValue = key.get() % numHashPartitions;
				Put partitionPut = new Put(Bytes.toBytes("hash"+hashValue)); //e.g., hash0, hash3
				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), null);	//is null correct?
//				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), Bytes.toBytes(""));	
//				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), new byte[]{});	
//				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), Bytes.toBytes(true));
				table.put(partitionPut);
			}
			
//			table.close();
		}
	}
	
	public static class HBaseStoreWithNumOutedgesReduce extends Reducer<IntWritable, Text, Text, Text> {
		private int mode;	//1 for outgoing, 2 for incoming, 3 for both
		private String tableName;	//HBase table name
		private HTable table; 
		private int numHashPartitions;
		

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.mode = Integer.parseInt(context.getConfiguration().get("mode"));
			System.out.println("#mode: " + this.mode);
			this.tableName = context.getConfiguration().get("table");
			System.out.println("HBase table: " + this.tableName);
			this.numHashPartitions = Integer.parseInt(context.getConfiguration().get("numHashPartitions"));
			System.out.println("Stroing hash partitions => #partitions: " + this.numHashPartitions);
			
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, this.tableName);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.close();
			super.cleanup(context);
		}

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			Set<Integer> outSet = new HashSet<Integer>();
			Set<Integer> inSet = new HashSet<Integer>();
			
			for(Text value : values) {	//for each non-anchor vertex
				if(value.toString().startsWith("O")) {
					outSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else if(value.toString().startsWith("I")) {
					inSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else {
					System.out.println("Something wrong: " + value.toString());
					continue;
				}
			}
			
			String outString = "";
			for(int out : outSet)
				outString += out + " ";
			String inString = "";
			for(int in : inSet)
				inString += in + " ";
			
			Put put = new Put(Bytes.toBytes(key.get()));
			if(mode == 1 || mode == 3) {	//outgoing or both
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("out"), Bytes.toBytes(outString));
			} 
			if(mode == 2 || mode == 3) {	//incoming or both
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("in"), Bytes.toBytes(inString));
			} 
			table.put(put);
			
			if(numHashPartitions > 0) {	//store a vertex list for each hash value (partition)
				int hashValue = key.get() % numHashPartitions;
				Put partitionPut = new Put(Bytes.toBytes("hash"+hashValue)); //e.g., hash0, hash3
				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()+" "+outSet.size()), null);	
				table.put(partitionPut);
			}
			
//			table.close();
		}
	}
	
	public static class HBaseStoreForRangeScansReduce extends Reducer<IntWritable, Text, Text, Text> {
		private int mode;	//1 for outgoing, 2 for incoming, 3 for both
		private String tableName;	//HBase table name
		private HTable table; 
		private int numHashPartitions;
		private HashMap<Integer, Integer> partitionList = new HashMap<Integer, Integer>();
		private String partitionFile;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.mode = Integer.parseInt(context.getConfiguration().get("mode"));
			System.out.println("#mode: " + this.mode);
			this.tableName = context.getConfiguration().get("table");
			System.out.println("HBase table: " + this.tableName);
			this.numHashPartitions = Integer.parseInt(context.getConfiguration().get("numHashPartitions"));
			System.out.println("Stroing hash partitions => #partitions: " + this.numHashPartitions);
			this.partitionFile=context.getConfiguration().get("partitionFile");
			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, this.tableName);
			fillPartitionList();
		}
		public void fillPartitionList() {
			BufferedReader br = null;
			String currentLine = null;
			String[] splits;
			try {
				br = new BufferedReader(new FileReader(partitionFile));
				while ((currentLine = br.readLine()) != null) {
					splits = currentLine.split(",");
					partitionList.put(Integer.parseInt(splits[0]),
							Integer.parseInt(splits[1]));
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.close();
			super.cleanup(context);
		}

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			Set<Integer> outSet = new HashSet<Integer>();
			Set<Integer> inSet = new HashSet<Integer>();
			
			for(Text value : values) {	//for each non-anchor vertex
				if(value.toString().startsWith("O")) {
					outSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else if(value.toString().startsWith("I")) {
					inSet.add(Integer.parseInt(value.toString().substring(2).trim()));
				} else {
					System.out.println("Something wrong: " + value.toString());
					continue;
				}
			}
			
//			String outString = "";
			StringBuilder outString = new StringBuilder(); 
			for(int out : outSet) {
//				outString += out + " ";
				outString.append(out +"-"+partitionList.get(out) +" ");
				
			}
//			String inString = "";
			StringBuilder inString = new StringBuilder();
			for(int in : inSet) {
//				inString += in + " ";
				inString.append(in + " ");
			}
			
			int numDigits = ((numHashPartitions-1)+"").length();
			String format = "%0" + numDigits + "d-%d";	//"partitionID-vertexID"
			//int hashValue = key.get() % numHashPartitions;
			int hashValue = partitionList.get(key.get());
			//partitionList

			Put put = new Put(Bytes.toBytes(String.format(format, hashValue, key.get())));
			if(mode == 1 || mode == 3) {	//outgoing or both
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("out"), Bytes.toBytes(outString.toString()));
			} 
			if(mode == 2 || mode == 3) {	//incoming or both
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("in"), Bytes.toBytes(inString.toString()));
			} 
			table.put(put);
			
			/*if(numHashPartitions > 0) {	//store a vertex list for each hash value (partition)
				Put partitionPut = new Put(Bytes.toBytes("hash"+hashValue)); //e.g., hash0, hash3
				partitionPut.add(Bytes.toBytes("cf"), Bytes.toBytes(key.get()), null);	//is null correct?
				table.put(partitionPut);
			}*/
			
//			table.close();
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
		options.addOption(OptionBuilder.withArgName("#reducers").
				hasArg().
				isRequired().
				withDescription("number of reducers").
				create("numReducers"));
		options.addOption(OptionBuilder.withArgName("mode").
				hasArg().
				isRequired().
				withDescription("1 for outgoing, 2 for incoming, 3 for both").
				create("mode"));
		options.addOption(OptionBuilder.withArgName("name").
				hasArg().
				isRequired().
				withDescription("HBase table name").
				create("table"));
		options.addOption(OptionBuilder.withArgName("partitionFile").
				hasArg().
				isRequired().
				withDescription("Partitioning dictionary file").
				create("partitionFile"));
		options.addOption(OptionBuilder.withArgName("#partitions").
				hasArg().
				withDescription("(optional) store a vertex list for each hash value (e.g., row key: hash3)").
				create("numHashPartitions"));
		options.addOption(OptionBuilder.
				withDescription("use a separate column for storing each neighbor").
				create("separate"));
		options.addOption(OptionBuilder.
				withDescription("when storing the vertext list, store with the number of out-edges of each vertex").
				create("withNumOutedges"));
		options.addOption(OptionBuilder.
				withDescription("row keys include partition number (e.g., 01-1242) for using HBase range scans").
				create("rangeScan"));

		try {
			Calendar start = Calendar.getInstance();

			CommandLine line = parser.parse(options, otherArgs);
			String inputDirectory = line.getOptionValue("input");
			String outputDirectory = line.getOptionValue("output");
			String table = line.getOptionValue("table");
			String partFile = line.getOptionValue("partitionFile");
			int numReducers = Integer.parseInt(line.getOptionValue("numReducers"));
			int mode = Integer.parseInt(line.getOptionValue("mode"));
			
			int numHashPartitions = 0;
			if(line.hasOption("numHashPartitions"))
				numHashPartitions = Integer.parseInt(line.getOptionValue("numHashPartitions"));

			if(numReducers < 1)
				Common.printUsage(options);
			if(mode < 1 || mode > 3)
				Common.printUsage(options);

			{
				Configuration confVBGenerator = new Configuration();
				confVBGenerator.set("mode", mode+"");
				confVBGenerator.set("table", table);
				confVBGenerator.set("numHashPartitions", numHashPartitions+"");
				confVBGenerator.set("partitionFile",partFile);
				Job jobVBGenerator = new Job(confVBGenerator);
				TableMapReduceUtil.addDependencyJars(jobVBGenerator);
				jobVBGenerator.setJobName("Storing Graph to HBase : mode " + mode);
				FileInputFormat.addInputPath(jobVBGenerator, new Path(inputDirectory));	//input
				FileOutputFormat.setOutputPath(jobVBGenerator, new Path(outputDirectory));	//output

				jobVBGenerator.setJarByClass(HBaseGraphStoreJob.class);
				jobVBGenerator.setMapperClass(HBaseStoreMapper.class);
				if(line.hasOption("separate"))
					jobVBGenerator.setReducerClass(HBaseStoreUsingSeparateColumnsReduce.class);
				else if(line.hasOption("withNumOutedges"))
					jobVBGenerator.setReducerClass(HBaseStoreWithNumOutedgesReduce.class);
				else if(line.hasOption("rangeScan"))
					jobVBGenerator.setReducerClass(HBaseStoreForRangeScansReduce.class);
				else
					jobVBGenerator.setReducerClass(HBaseStoreReduce.class);
				jobVBGenerator.setNumReduceTasks(numReducers);

				jobVBGenerator.setInputFormatClass(TextInputFormat.class);
				jobVBGenerator.setOutputFormatClass(TextOutputFormat.class);
				jobVBGenerator.setMapOutputKeyClass(IntWritable.class);
				jobVBGenerator.setMapOutputValueClass(Text.class);
				jobVBGenerator.setOutputKeyClass(Text.class);
				jobVBGenerator.setOutputValueClass(Text.class);

				jobVBGenerator.waitForCompletion(true);
			}

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
