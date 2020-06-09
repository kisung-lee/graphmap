package edu.gatech.cc.vbp.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class CommandManager {

	
	public static void executeLocalCommand(String command, boolean isPrintOutput, boolean isVerbose) {
		ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
		pb.redirectErrorStream(true);
		try {
			long start = Calendar.getInstance().getTime().getTime();
//			System.out.println("Executing " + pb.command() + " ...");
			Process p = pb.start();
			p.waitFor();
			
			if(isVerbose)
				System.out.println(String.format("Execution of %s: %d milliseconds", pb.command(), Calendar.getInstance().getTime().getTime()-start));
			if(isPrintOutput) {
				InputStream is = p.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String line;
				while((line = br.readLine()) != null) {
					System.out.println(line);
				}
			}
			p.destroy();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static long executeMultipleRemoteCommands(List<String> destinations, List<String> commands, boolean isVerbose) {
		List<Thread> threads = new ArrayList<Thread>();
		long start = Calendar.getInstance().getTime().getTime();
		for(int i = 0; i < destinations.size(); i++) {
			Runnable oneCommand = new CommandExecuteRunnable(destinations.get(i), commands.get(i), isVerbose);
			Thread thread = new Thread(oneCommand);
			threads.add(thread);
			thread.start();
		}
		
		for(Thread thread : threads)
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		
		long processingTime = Calendar.getInstance().getTime().getTime()-start;
		if(isVerbose)
			System.out.println(String.format("Total Execution Time of %d files: %d milliseconds", destinations.size(), processingTime));
		return processingTime;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*transmitFileViaSCP("lubm_1.n3", "greg@localhost:~/Documents");
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		List<String> filenames = Arrays.asList("lubm_0.n3", "lubm_2.n3");
		List<String> destinations = Arrays.asList("greg@localhost:~/Documents", "greg@localhost:~/Documents");
		transmitMultipleFilesViaSCP(filenames, destinations);*/
		
//		executeRemoteCommand("localhost", "ls -lh", false);
		
		executeLocalCommand("ssh localhost ls -lh", true, true);
	}

}
