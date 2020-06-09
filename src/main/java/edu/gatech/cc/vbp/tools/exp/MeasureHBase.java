package edu.gatech.cc.vbp.tools.exp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.NavigableMap;
import java.util.StringTokenizer;

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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.gatech.cc.vbp.tools.Common;

@SuppressWarnings("deprecation")
public class MeasureHBase {
	
	private HTable table = null;
	
	public MeasureHBase(String tableName) {
		Configuration config = HBaseConfiguration.create();
		try {
			table = new HTable(config, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void closeTable() {
		try {
			if(this.table != null)
				this.table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private void readRow(Object rowKey, int numRuns) throws IOException {
		for(int i = 0; i < numRuns; i++) {
			Calendar start = Calendar.getInstance();
			
			List<Integer> values = new ArrayList<Integer>();
			
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
			
			System.out.println(String.format("run %d (%d): %d milliseconds", i, values.size(), 
					Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis()));
		}
	}
	
	private void readColumn(Object rowKey, int numRuns, String columnName) throws IOException {
		for(int i = 0; i < numRuns; i++) {
			Calendar start = Calendar.getInstance();
			
			List<Integer> values = new ArrayList<Integer>();
			
			Get get = null;
			if(rowKey instanceof String)
				get = new Get(Bytes.toBytes((String)rowKey));
			else if(rowKey instanceof Integer)
				get = new Get(Bytes.toBytes((Integer)rowKey));
			else
				return;
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(columnName));
			Result result = table.get(get);
			byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(columnName));
			
			if(value != null) {
				String outEdgesStr = Bytes.toString(value);
				StringTokenizer st = new StringTokenizer(outEdgesStr);
				while(st.hasMoreTokens()) {
					int borderVertexID = Integer.parseInt(st.nextToken());
					values.add(borderVertexID);
				}
			}
			
			System.out.println(String.format("run %d (%d): %d milliseconds", i, values.size(), 
					Calendar.getInstance().getTimeInMillis()-start.getTimeInMillis()));
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
		options.addOption(OptionBuilder.withArgName("runs").
				hasArg().
				withDescription("# runs").
				create("run"));
		options.addOption(OptionBuilder.withArgName("column name").
				hasArg().
				withDescription("read a single column").
				create("column"));
		
		
		try {
			CommandLine line = parser.parse(options, otherArgs);
			String tableName = line.getOptionValue("table");
			int mode = Integer.parseInt(line.getOptionValue("mode"));
			int numRuns = 3;
			if(line.hasOption("run"))
				numRuns = Integer.parseInt(line.getOptionValue("run"));
			
			MeasureHBase measureHBase = new MeasureHBase(tableName);
			
			if(line.hasOption("column")) {
				String column = line.getOptionValue("column");
				if(mode == 1) {
					String key = line.getOptionValue("key");
					measureHBase.readColumn(key, numRuns, column);
				} else if(mode == 2) {
					Integer key = Integer.parseInt(line.getOptionValue("key"));
					measureHBase.readColumn(key, numRuns, column);
				}
			} else {
				if(mode == 1) {
					String key = line.getOptionValue("key");
					measureHBase.readRow(key, numRuns);
				} else if(mode == 2) {
					Integer key = Integer.parseInt(line.getOptionValue("key"));
					measureHBase.readRow(key, numRuns);
				}
			}
			
			measureHBase.closeTable();
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}

}
