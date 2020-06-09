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

@SuppressWarnings("deprecation")
public class HBaseGraphStoreJob_BK {

	/**
	 * Input: input graph (each line: start_vertex_id end_vertex_id) Output:
	 * output1
	 * 
	 * This Hadoop job does 1.
	 */
	public static class HBaseStoreMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable keyInt = new IntWritable();
		private Text valueText = new Text();
		private String[] splits;
		private int numMetisPartitions;
		private int numDigits;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.numMetisPartitions = Integer.parseInt(context
					.getConfiguration().get("numMetisPartitions"));
			numDigits = ((numMetisPartitions - 1) + "").length();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String edgeStr = value.toString().trim();
			if (edgeStr.startsWith("%")) // comment
				return;

			splits = value.toString().split(",");
			keyInt.set(Integer.parseInt(splits[0]));
			valueText.set(String.format("%0" + numDigits + "d",
					Integer.parseInt(splits[1]))
					+ "-," + splits[2].replaceAll("\\t", " "));
			context.write(keyInt, valueText);
		}
	}

	public static class HBaseStoreForRangeScansReduce extends
			Reducer<IntWritable, Text, Text, Text> {
		private String tableName; // HBase table name
		private HTable table;
		private int numMetisPartitions;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			this.tableName = context.getConfiguration().get("table");
			System.out.println("HBase table: " + this.tableName);
			this.numMetisPartitions = Integer.parseInt(context
					.getConfiguration().get("numMetisPartitions"));
			System.out.println("Stroing hash partitions => #partitions: "
					+ this.numMetisPartitions);

			Configuration config = HBaseConfiguration.create();
			table = new HTable(config, this.tableName);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.close();
			super.cleanup(context);
		}

		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String[] metisList;

			for (Text value : values) { // for each non-anchor vertex
				metisList = value.toString().split(",");
				Put put = new Put(Bytes.toBytes(metisList[0] + key.toString()));
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("out"),
						Bytes.toBytes(metisList[1]));
				table.put(put);
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
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("dir").hasArg()
				.isRequired().withDescription("input directory (HDFS)")
				.create("input"));
		options.addOption(OptionBuilder.withArgName("dir").hasArg()
				.isRequired().withDescription("output directory (HDFS)")
				.create("output"));
		options.addOption(OptionBuilder.withArgName("#reducers").hasArg()
				.isRequired().withDescription("number of reducers")
				.create("numReducers"));
		options.addOption(OptionBuilder.withArgName("name").hasArg()
				.isRequired().withDescription("HBase table name")
				.create("table"));
		options.addOption(OptionBuilder
				.withArgName("#partitions")
				.hasArg()
				.withDescription(
						"(optional) store a vertex list for each hash value (e.g., row key: hash3)")
				.create("numMetisPartitions"));
		options.addOption(OptionBuilder.withDescription(
				"use a separate column for storing each neighbor").create(
				"separate"));
		options.addOption(OptionBuilder
				.withDescription(
						"when storing the vertext list, store with the number of out-edges of each vertex")
				.create("withNumOutedges"));
		options.addOption(OptionBuilder
				.withDescription(
						"row keys include partition number (e.g., 01-1242) for using HBase range scans")
				.create("rangeScan"));

		try {
			Calendar start = Calendar.getInstance();

			CommandLine line = parser.parse(options, otherArgs);
			String inputDirectory = line.getOptionValue("input");
			String outputDirectory = line.getOptionValue("output");
			String table = line.getOptionValue("table");

			int numReducers = Integer.parseInt(line
					.getOptionValue("numReducers"));

			int numMetisPartitions = 0;
			if (line.hasOption("numMetisPartitions"))
				numMetisPartitions = Integer.parseInt(line
						.getOptionValue("numMetisPartitions"));

			if (numReducers < 1)
				Common.printUsage(options);

			Configuration confVBGenerator = new Configuration();
			confVBGenerator.set("table", table);
			confVBGenerator.set("numMetisPartitions", numMetisPartitions + "");

			Job jobVBGenerator = new Job(confVBGenerator);
			TableMapReduceUtil.addDependencyJars(jobVBGenerator);
			FileInputFormat.addInputPath(jobVBGenerator, new Path(
					inputDirectory)); // input
			FileOutputFormat.setOutputPath(jobVBGenerator, new Path(
					outputDirectory)); // output

			jobVBGenerator.setJarByClass(HBaseGraphStoreJob_BK.class);
			jobVBGenerator.setMapperClass(HBaseStoreMapper.class);

			// if (line.hasOption("rangeScan"))
			jobVBGenerator.setReducerClass(HBaseStoreForRangeScansReduce.class);

			jobVBGenerator.setNumReduceTasks(numReducers);

			jobVBGenerator.setInputFormatClass(TextInputFormat.class);
			jobVBGenerator.setOutputFormatClass(TextOutputFormat.class);
			jobVBGenerator.setMapOutputKeyClass(IntWritable.class);
			jobVBGenerator.setMapOutputValueClass(Text.class);
			jobVBGenerator.setOutputKeyClass(Text.class);
			jobVBGenerator.setOutputValueClass(Text.class);

			jobVBGenerator.waitForCompletion(true);

			System.out.println(String.format(
					"Total processing time: %d seconds", (Calendar
							.getInstance().getTimeInMillis() - start
							.getTimeInMillis()) / 1000));

		} catch (ParseException e) {
			System.err.println("Parsing failed.  Reason: " + e.getMessage());
			Common.printUsage(options);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

}
