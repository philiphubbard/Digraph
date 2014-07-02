package com.philiphubbard.digraph;

import org.apache.hadoop.io.Text;

public class MREdge {

	public static boolean getIsMREdge(Text text) {
		int header = text.charAt(0);
		short type = (short) (header >>> (16 - NUM_TYPE_BITS));
		return (type == TEXT_TYPE_ID);
	}
	
	public MREdge(int from, int to) {
		this.from = from;
		this.to = to;
	}
	
	public MREdge(Text text) {
		String s = text.toString();
		// HEY!!
		//char header = s.charAt(0);
		s = s.substring(1);
		String[] tokens = s.split(SEPARATOR);
		// HEY!! Add error handling for tokens not being the right length.
		from = Integer.parseInt(tokens[0]);
		to = Integer.parseInt(tokens[1]);
	}
	
	public Text toText() {
		StringBuilder s = new StringBuilder();
		setHeader(s);
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
	
	protected void setHeader(StringBuilder s) {
		char header = (char) (TEXT_TYPE_ID << (16 - NUM_TYPE_BITS));
		s.append(header);
	}
	
	private static final short NUM_TYPE_BITS = 8;
	private static final short TEXT_TYPE_ID = 2;

	private int from;
	private int to;
	private static final String SEPARATOR = ",";
}
