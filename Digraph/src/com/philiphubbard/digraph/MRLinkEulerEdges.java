package com.philiphubbard.digraph;

import com.philiphubbard.digraph.MRVertex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// HEY!! Reformat the following:
// Mapper input key: vertex id.
// Mapper input value: serialized vertex with "to" and "from".
// Mapper output value: an edge in the Euler tour, either coming into the vertex or going out, 
// with the vertex made unique by adding a serial number to its ID.
// Mapper output key: the edge without the serial number added.
// Reducer output value: the edge with its "to" and "from" including serial numbers if appropriate.

public class MRLinkEulerEdges {

	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			MRVertex vertex = new MRVertex(value);
			
			int serialNumPower = context.getConfiguration().getInt("serialNumPower", DEFAULT_SERIAL_NUM_POWER);
			int serialNumBase = 0;
			
			MRVertex.AdjacencyIterator itFrom = vertex.createFromAdjacencyIterator();
			MRVertex.AdjacencyIterator itTo = vertex.createToAdjacencyIterator();
			
			// TODO: Check for serial number overflow.
			
			if (itFrom.done() && (!itTo.done())) {
				// Source
				for (int to = itTo.begin(); !itTo.done(); to = itTo.next()) {
					MREdge edgeKey = new MREdge(vertex.id(), to);
					int serialNum = serialNumBase++ << serialNumPower;
					int current = vertex.id() + serialNum;
					MREdge edgeSerialNum = new MREdge(current, to);
					context.write(edgeKey.toText(), edgeSerialNum.toText());
				}
			}
			else if ((!itFrom.done()) && itTo.done()) {
				// Sink
				for (int from = itFrom.begin(); !itFrom.done(); from = itFrom.next()) {
					MREdge edgeKey = new MREdge(from, vertex.id());
					int serialNum = serialNumBase++ << serialNumPower;
					int current = vertex.id() + serialNum;
					MREdge edgeSerialNum = new MREdge(from, current);
					context.write(edgeKey.toText(), edgeSerialNum.toText());					
				}
			}
			else {
				int from = itFrom.begin();
				int to = itTo.begin();
				while ((!itFrom.done()) && (!itTo.done())) {
					int serialNum = serialNumBase++ << serialNumPower;
					int current = vertex.id() + serialNum;

					MREdge edgeKey1 = new MREdge(from, vertex.id());
					MREdge edgeSerialNum1 = new MREdge(from, current);
					context.write(edgeKey1.toText(), edgeSerialNum1.toText());
					
					MREdge edgeKey2 = new MREdge(vertex.id(), to);
					MREdge edgeSerialNum2 = new MREdge(current, to);
					context.write(edgeKey2.toText(), edgeSerialNum2.toText());
					
					from = itFrom.next();
					to = itTo.next();
				}
			}
		}
	}

	// HEY!! What should the key be?
	public static class Reducer 
	extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			MREdge edgeKey = new MREdge(key);
			int from = edgeKey.getFrom();
			int to = edgeKey.getTo();
			
			for (Text text : values) {
				MREdge other = new MREdge(text);
				if (other.getFrom() > from)
					from = other.getFrom();
				if (other.getTo() > to)
					to = other.getTo();
			}
			
			// HEY!! Is it correct that the key E(from, to) can have as its values
			// only E(from, to), E(from', to), E(from, to') and E(from', to')?
			// Can we have edges with multiplicities?
			
			MREdge edgeValue = new MREdge(from, to);
			context.write(key, edgeValue.toText());
		}
	}
	
	private static final int DEFAULT_SERIAL_NUM_POWER = 12;
}
