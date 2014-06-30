package com.philiphubbard.digraph;

import org.apache.hadoop.io.Text;

public class MREdge {

	public MREdge(int from, int to) {
		this.from = from;
		this.to = to;
	}
	
	public MREdge(Text text) {
		String s = text.toString();
		String[] tokens = s.split(SEPARATOR);
		// HEY!! Add error handling for tokens not being the right length.
		from = Integer.parseInt(tokens[0]);
		to = Integer.parseInt(tokens[1]);
	}
	
	public Text toText() {
		StringBuilder s = new StringBuilder();
		s.append(from);
		s.append(SEPARATOR);
		s.append(to);
		return new Text(s.toString());
	}
	
	public int getFrom() {
		return from;
	}
	
	public int getTo() {
		return to;
	}
	
	private int from;
	private int to;
	private static final String SEPARATOR = ",";
}
