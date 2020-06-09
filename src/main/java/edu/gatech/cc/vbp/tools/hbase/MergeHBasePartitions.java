package edu.gatech.cc.vbp.tools.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.gatech.cc.vbp.tools.Common;

@SuppressWarnings("deprecation")
public class MergeHBasePartitions {
	
	private static void mergeHBasePartitions(String tableName, String columnName,
			int numPartitions) {
		Configuration config = HBaseConfiguration.create();
		HTable table = null;
		try {
			Calendar start = Calendar.getInstance();
			
			table = new HTable(config, tableName);
			
			for(int i=0; i < numPartitions; i++) {
				List<Integer> values = new ArrayList<Integer>();
				
				//read
				System.out.println("reading ... hash" + i);
				Get get = new Get(Bytes.toBytes("hash"+i));
				get.addFamily(Bytes.toBytes("cf"));
				Result result = table.get(get);
				
				NavigableMap<byte[], byte[]> valueMap = result.getFamilyMap(Bytes.toBytes("cf"));
				for(byte[] column : valueMap.keySet()) {	//for each vertex of this partition
					int vertexID = Bytes.toInt(column);
					values.add(vertexID);
				}
				System.out.println("reading ... hash" + i + " done! => " + values.size());
				
				System.out.println("storing ... hash" + i);
				//store
				Put put = new Put(Bytes.toBytes("newhash"+i));
				StringBuilder mergeString = new StringBuilder("");
				for(int value : values) {
					mergeString.append(value + " ");
				}
				put.add(Bytes.toBytes("cf"), Bytes.toBytes(columnName), Bytes.toBytes(mergeString.toString().trim()));
				table.put(put);
				System.out.println("storing ... hash" + i + " done!");
			}
			
			System.out.println(String.format("run: %d milliseconds", 
					Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis()));
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(table != null)
					table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void mergeHBasePartitionsStringType(String tableName, String columnName,
			int numPartitions) {
		Configuration config = HBaseConfiguration.create();
		HTable table = null;
		try {
			Calendar start = Calendar.getInstance();
			
			table = new HTable(config, tableName);
			
			for(int i=0; i < numPartitions; i++) {
				List<String> values = new ArrayList<String>();
				
				//read
				System.out.println("reading ... hash" + i);
				Get get = new Get(Bytes.toBytes("hash"+i));
				get.addFamily(Bytes.toBytes("cf"));
				Result result = table.get(get);
				
				NavigableMap<byte[], byte[]> valueMap = result.getFamilyMap(Bytes.toBytes("cf"));
				for(byte[] column : valueMap.keySet()) {	//for each vertex of this partition
					String vertexIDWithNumOutedges = Bytes.toString(column);
					values.add(vertexIDWithNumOutedges);
				}
				System.out.println("reading ... hash" + i + " done! => " + values.size());
				
				System.out.println("storing ... hash" + i);
				//store
				Put put = new Put(Bytes.toBytes("newhash"+i));
				StringBuilder mergeString = new StringBuilder("");
				for(String value : values) {
					mergeString.append(value + " ");
				}
				put.add(Bytes.toBytes("cf"), Bytes.toBytes(columnName), Bytes.toBytes(mergeString.toString().trim()));
				table.put(put);
				System.out.println("storing ... hash" + i + " done!");
			}
			
			System.out.println(String.format("run: %d milliseconds", 
					Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis()));
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(table != null)
					table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void mergeHBasePartitionsForRangeScans(String tableName, String columnName,
			int numPartitions) {
		Configuration config = HBaseConfiguration.create();
		HTable table = null;
		try {
			Calendar start = Calendar.getInstance();
			
			table = new HTable(config, tableName);
			
			for(int i=0; i < numPartitions; i++) {
				List<Integer> values = new ArrayList<Integer>();
				
				//read
				System.out.println("reading ... hash" + i);
				Get get = new Get(Bytes.toBytes("hash"+i));
				get.addFamily(Bytes.toBytes("cf"));
				Result result = table.get(get);
				
				NavigableMap<byte[], byte[]> valueMap = result.getFamilyMap(Bytes.toBytes("cf"));
				for(byte[] column : valueMap.keySet()) {	//for each vertex of this partition
					int vertexID = Bytes.toInt(column);
					values.add(vertexID);
				}
				System.out.println("reading ... hash" + i + " done! => " + values.size());
				
				//store
				System.out.println("storing ... hash" + i);
				int numDigits = ((numPartitions-1)+"").length();
				String format = "%0" + numDigits + "d-hash";	//"partitionID-hash"
				
				Put put = new Put(Bytes.toBytes(String.format(format, i)));
				StringBuilder mergeString = new StringBuilder("");
				for(int value : values) {
					mergeString.append(value + " ");
				}
				put.add(Bytes.toBytes("cf"), Bytes.toBytes(columnName), Bytes.toBytes(mergeString.toString().trim()));
				table.put(put);
				System.out.println("storing ... hash" + i + " done!");
				
				//delete
				System.out.println("deleting ... hash" + i);
				Delete delete = new Delete(Bytes.toBytes("hash"+i));
				table.delete(delete);
				System.out.println("deleting ... hash" + i + " done!");
			}
			
			System.out.println(String.format("run: %d milliseconds", 
					Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis()));
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(table != null)
					table.close();
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
		options.addOption(OptionBuilder.withArgName("name").
				hasArg().
				isRequired().
				withDescription("HBase table name").
				create("table"));
		options.addOption(OptionBuilder.withArgName("num").
				hasArg().
				isRequired().
				withDescription("# hash partitions").
				create("numPartitions"));
		options.addOption(OptionBuilder.withArgName("column name").
				hasArg().
				isRequired().
				withDescription("column name").
				create("column"));
		options.addOption(OptionBuilder.
				withDescription("column names are string").
				create("string"));
		options.addOption(OptionBuilder.
				withDescription("merge partitions for range scan (e.g., key: 01-hash").
				create("range"));

		try {
			CommandLine line = parser.parse(options, otherArgs);
			String tableName = line.getOptionValue("table");
			String column = line.getOptionValue("column");
			int numPartitions = Integer.parseInt(line.getOptionValue("numPartitions"));
			if(line.hasOption("string"))
				mergeHBasePartitionsStringType(tableName, column, numPartitions);
			else if(line.hasOption("range"))
				mergeHBasePartitionsForRangeScans(tableName, column, numPartitions);
			else
				mergeHBasePartitions(tableName, column, numPartitions);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}

}
