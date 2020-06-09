package edu.gatech.cc.vbp.tools;

public class CommandExecuteRunnable implements Runnable {
	
	private String destination = null;
	private String command = null;
	private boolean isVerbose = true;

	public CommandExecuteRunnable(String destination, String command) {
		super();
		this.destination = destination;
		this.command = command;
	}
	
	public CommandExecuteRunnable(String destination, String command, boolean isVerbose) {
		this(destination, command);
		this.isVerbose = isVerbose;
	}

	@Override
	public void run() {
//		PartitionManager.executeRemoteCommand(destination, command, false);
		CommandManager.executeLocalCommand("ssh " + destination + " " + command, this.isVerbose, this.isVerbose);

	}

}
