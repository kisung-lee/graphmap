package edu.gatech.cc.vbp.graph;

import java.util.StringTokenizer;

public class UnweightedEdge {
	private int startVertexID;
	private int endVertexID;
	
	private boolean isValid = true;
	
	public UnweightedEdge(int startVertexID, int endVertexID) {
		super();
		this.startVertexID = startVertexID;
		this.endVertexID = endVertexID;
	}
	
	public UnweightedEdge(String edgeStr) {
		StringTokenizer st = new StringTokenizer(edgeStr);
		if(st.countTokens() != 2) {
			this.isValid = false;
			return;
		}
		
		try {
			this.startVertexID = Integer.parseInt(st.nextToken());
			this.endVertexID = Integer.parseInt(st.nextToken());
			
			if(this.startVertexID == this.endVertexID)	//currently, ignore self-edges
				this.isValid = false;
		} catch (NumberFormatException e) {
			e.printStackTrace();
			this.isValid = false;
		}
	}

	public int getStartVertexID() {
		return startVertexID;
	}

	public int getEndVertexID() {
		return endVertexID;
	}

	public boolean isValid() {
		return isValid;
	}
}
