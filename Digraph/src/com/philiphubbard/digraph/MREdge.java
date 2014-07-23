package com.philiphubbard.digraph;

import org.apache.hadoop.io.BytesWritable;

public class MREdge {

	public static boolean getIsMREdge(BytesWritable writable) {
		byte [] bytes = writable.getBytes();
		return (bytes[0] == WRITABLE_TYPE_ID);
	}
	
	public MREdge(int from, int to) {
		this.from = from;
		this.to = to;
	}
	
	public MREdge(BytesWritable writable) {
		byte [] bytes = writable.getBytes();
		from = getInt(bytes, 1);
		to = getInt(bytes, 5);
	}

	public BytesWritable toWritable() {
		int numBytes = 1 + 4 + 4;
		byte[] result = new byte[numBytes];
		
		int i = putHeader(result);
		i = putInt(from, result, i);
		i = putInt(to, result, i);
		
		return new BytesWritable(result);
	}
	
	public int getFrom() {
		return from;
	}
	
	public int getTo() {
		return to;
	}
	
	protected int putHeader(byte[] array) {
		array[0] = WRITABLE_TYPE_ID;
		return 1;
	}
	
	private int putInt(int value, byte[] array, int i) {
		// 32 bits
		array[i] = (byte) ((value & 0xff000000) >>> 24);
		array[i+1] = (byte) ((value & 0xff0000) >>> 16);
		array[i+2] = (byte) ((value & 0xff00) >>> 8);
		array[i+3] = (byte) (value & 0xff);
		return i + 4;
	}
	
	private int getInt(byte[] array, int i) {
		int result = 0;
		result |= ((0xff & ((int) array[i])) << 24);
		result |= ((0xff & ((int) array[i+1])) << 16);
		result |= ((0xff & ((int) array[i+2])) << 8);
		result |= (0xff & ((int) array[i+3]));
		return result;
	}
	
	private static final byte WRITABLE_TYPE_ID = 2;

	private int from;
	private int to;
}
