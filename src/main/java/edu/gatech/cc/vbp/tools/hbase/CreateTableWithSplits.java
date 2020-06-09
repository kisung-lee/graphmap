package edu.gatech.cc.vbp.tools.hbase;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.gatech.cc.vbp.tools.Common;

@SuppressWarnings("deprecation")
public class CreateTableWithSplits {
	enum SPLIT_MODE {
		SIMPLE_UNIFORM,
		HASH_PARTITIONING,
	}
	
	private static byte[][] generateSimpleUniformSplits(int numSplits, int maxVertexID) {
		int increment = maxVertexID / numSplits;
		
		byte[][] splits = new byte[numSplits-1][];
		for(int i=0; i < numSplits-1; i++) {
			splits[i] = Bytes.toBytes(increment*(i+1));
			System.out.println("split point " + i + ": " + increment*(i+1));
		}
		
		return splits;
	}
	
	private static byte[][] generateHashPartitioningSplits(int numSplits, int numHashPartitions) {
		int numDigits = ((numHashPartitions-1)+"").length();
		String format = "%0" + numDigits + "d-";	//"partitionID-"
		
		int increment = numHashPartitions / numSplits;	
		if(numSplits > numHashPartitions)
			increment = 1;	//actual #splits can be smaller 
		
		byte[][] splits = new byte[numSplits-1][];
		for(int i=0; i < numSplits-1; i++) {
			splits[i] = Bytes.toBytes(String.format(format, increment*(i+1)));
			System.out.println("split point " + i + ": " + String.format(format, increment*(i+1)));
		}
		
		return splits;
	}
	
	private static void createTableWithSplits(String tableName, int numSplits, SPLIT_MODE mode, int splitParameter) {
		Configuration config = HBaseConfiguration.create();
		try {
			@SuppressWarnings("resource")
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			HColumnDescriptor colDesc = new HColumnDescriptor("cf");
			tableDesc.addFamily(colDesc);
			
			//For Consistent Splits
			tableDesc.setMaxFileSize(Integer.MAX_VALUE);
			
			//generate test split points
			/*byte[][] splits = new byte[numSplits-1][];
			for(int i=0; i < numSplits-1; i++)
				splits[i] = Bytes.toBytes(1000*(i+1));*/
			
			byte[][] splits = null;
			switch (mode) {
			case SIMPLE_UNIFORM:
				splits = generateSimpleUniformSplits(numSplits, splitParameter);
				break;
			case HASH_PARTITIONING:
				splits = generateHashPartitioningSplits(numSplits, splitParameter);
				break;
			default:
				break;
			}
			admin.createTable(tableDesc, splits);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
				withDescription("# regions").
				create("numSplits"));
		options.addOption(OptionBuilder.withArgName("id").
				hasArg().
				withDescription("maximum vertex ID (use the simple uniform splits)").
				create("maxID"));
		options.addOption(OptionBuilder.withArgName("num").
				hasArg().
				withDescription("# hash partitions (use the hash partitioning splits)").
				create("numPartitions"));
		
		try {
			CommandLine line = parser.parse(options, otherArgs);
			String tableName = line.getOptionValue("table");
			int numSplits = Integer.parseInt(line.getOptionValue("numSplits"));
			if(line.hasOption("maxID")) {
				int maxID = Integer.parseInt(line.getOptionValue("maxID"));
				createTableWithSplits(tableName, numSplits, SPLIT_MODE.SIMPLE_UNIFORM, maxID);
			} else if(line.hasOption("numPartitions")) {
				int numPartitions = Integer.parseInt(line.getOptionValue("numPartitions"));
				createTableWithSplits(tableName, numSplits, SPLIT_MODE.HASH_PARTITIONING, numPartitions);
			} else
				Common.printUsage(options);
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			Common.printUsage(options);
		}

	}

}
