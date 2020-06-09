package edu.gatech.cc.vbp.tools.hbase;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.gatech.cc.vbp.tools.Common;

@SuppressWarnings("deprecation")
public class MergeHBaseRow {
	
	private static void mergeHBaseRow(String tableName, Object rowKey, String columnName) {
		Configuration config = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(config, tableName);

			List<Integer> values = new ArrayList<Integer>();

			//read
			System.out.println("reading ...");
			Get get = null;
			if(rowKey instanceof String)
				get = new Get(Bytes.toBytes((String)rowKey));
			else if(rowKey instanceof Integer)
				get = new Get(Bytes.toBytes((Integer)rowKey));
			else
				return;
			get.addFamily(Bytes.toBytes("cf"));
			Result result = table.get(get);

			NavigableMap<byte[], byte[]> valueMap = result.getFamilyMap(Bytes.toBytes("cf"));
			for(byte[] column : valueMap.keySet()) {	//for each anchor vertex (column values are empty)
				int anchorVertexID = Bytes.toInt(column);
				values.add(anchorVertexID);
			}
			System.out.println("reading ... done! => " + values.size());
			
			System.out.println("storing ...");
			//store
			Put put = new Put(Bytes.toBytes("new"+rowKey));
			StringBuilder mergeString = new StringBuilder("");
//			String mergeString = "";
			for(int value : values) {
				mergeString.append(value + " ");
//				mergeString += value + " ";
			}
//			put.add(Bytes.toBytes("cf"), Bytes.toBytes("out"), Bytes.toBytes(mergeString.trim()));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("out"), Bytes.toBytes(mergeString.toString().trim()));
			table.put(put);
			System.out.println("storing ... done!");
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
		options.addOption(OptionBuilder.withArgName("mode").
				hasArg().
				isRequired().
				withDescription("1: string key, 2: int key ").
				create("mode"));
		options.addOption(OptionBuilder.withArgName("key").
				hasArg().
				isRequired().
				withDescription("HBase row key").
				create("key"));
		options.addOption(OptionBuilder.withArgName("column name").
				hasArg().
				isRequired().
				withDescription("column name").
				create("column"));

		try {
			CommandLine line = parser.parse(options, otherArgs);
			String tableName = line.getOptionValue("table");
			int mode = Integer.parseInt(line.getOptionValue("mode"));
			
			String column = line.getOptionValue("column");
			if(mode == 1) {
				String key = line.getOptionValue("key");
				mergeHBaseRow(tableName, key, column);
			} else if(mode == 2) {
				Integer key = Integer.parseInt(line.getOptionValue("key"));
				mergeHBaseRow(tableName, key, column);
			}
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}
	}

}
