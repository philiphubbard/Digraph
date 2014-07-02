package com.philiphubbard.digraph;

import com.philiphubbard.digraph.MRVertex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


// HEY!! What is needed for the this MR phase:
// Mapper reads in MRVertex instances.
// Mapper outputs MRVertex instances.
// Key is MRVertex.getMergeKey().  
// (Since key is separate from value, don't need anything like MRVertexMergeable.)
// Value is MRVertex.toText().  (Derived Sabe class will add MerString data.)
// Reducer reads merge keys and MRVertexMergeable values.
// Does merge if appropriate.
// Writes out MRVertex.toText() values: just 1 if merge, or 2 if didn't merge.
// Reducer output key does not matter? Use vertex ID?

// HEY!! Reformat the following:
// Mapper input key: vertex id.
// Mapper input value: serialized vertex with "to" and "from".
// Mapper output value: an edge in the Euler tour, either coming into the vertex or going out, 
// with the vertex made unique by adding a serial number to its ID.
// Mapper output key: the edge without the serial number added.
// Reducer output value: the edge with its "to" and "from" including serial numbers if appropriate.

public class MRCompressChains {

	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			if (MRVertex.getIsBranch(value))
				throw new IOException("MRCompressChains.Mapper.map(): input vertex is a branch");

			MRVertex vertex = new MRVertex(value);
			IntWritable keyOut = new IntWritable(vertex.getMergeKey());
			
			context.write(keyOut, vertex.toText(MRVertex.FORMAT_EDGES_TO));
		}
	}

	// HEY!! What should the key be?
	public static class Reducer 
	extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			MRVertex vertex1 = null;
			MRVertex vertex2 = null;
			
			for (Text text : values) {
				if (MRVertex.getIsBranch(text))
					throw new IOException("MRCompressChains.Reducer.reduce(): input vertex is a branch");

				// HEY!! Error checking
				if (vertex1 == null)
					vertex1 = new MRVertex(text);
				else
					vertex2 = new MRVertex(text);
			}
			
			if (vertex1 == null)
				throw new IOException("MRCompressChains.Reducer.reduce(): insufficient input vertices");
			
			if (vertex2 == null) {
				context.write(key, vertex1.toText(MRVertex.FORMAT_EDGES_TO));
			}
			else {
				int mergeKey = key.get();
				MRVertex vertexMerged = MRVertex.merge(vertex1, vertex2, mergeKey);
			
				if (vertexMerged.getId() == MRVertex.NO_VERTEX)
					throw new IOException("MRCompressChains.Reducer.reduce(): merge failed");
			
				context.write(key, vertexMerged.toText(MRVertex.FORMAT_EDGES_TO));
			}
		}
	}

}
