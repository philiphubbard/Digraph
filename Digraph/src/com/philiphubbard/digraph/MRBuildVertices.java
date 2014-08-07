// Copyright (c) 2014 Philip M. Hubbard
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// 
// http://opensource.org/licenses/MIT

package com.philiphubbard.digraph;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.philiphubbard.digraph.MRVertex;
import com.philiphubbard.digraph.MREdge;

// A mapper and reducer for a Hadoop map-reduce algorithm to collect the edges associated
// with a vertex, turning vertex descriptions with only the edges pointing out from the
// vertex into vertex descriptions that also include the edges pointing in from other
// vertices.

public class MRBuildVertices {

	// TODO: Consider switching from int to long for vertex identifiers.
	
	// Required setup.
	
	public static void setupJob(Job job, Path inputPath, Path outputPath) 
			throws IOException {
		job.setJarByClass(MRBuildVertices.class);
		job.setMapperClass(MRBuildVertices.Mapper.class);
		job.setCombinerClass(MRBuildVertices.Reducer.class);
		job.setReducerClass(MRBuildVertices.Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	}
	
	// 
	
	// Optional setup.
	
	public static void setPartitionBranchesChains(boolean doPartition) {
		partitionBranchesChains = doPartition;
	}
	
	public static boolean getPartitionBranchesChains() {
		return partitionBranchesChains;
	}
	
	public static void setIncludeFromEdges(boolean doInclude) {
		includeFromEdges = doInclude;
	}
	
	public static boolean getIncludeFromEdges() {
		return includeFromEdges;
	}

	// "Coverage" is the number of reads that include a part of the sequence.
	// An odd coverage is preferred, because the error correction scheme
	// dismisses as errors any candidates whose actual coverage is
	// less than ceiling(coverage / 2.0).
	
	public static final int DISABLE_COVERAGE_ERROR_CORRECTION = -1;
	
	public static void setCoverage(int coverage) {
		MRBuildVertices.coverage = coverage;
	}
	
	public static int getCoverage() {
		return coverage;
	}
	
	//
	
	// The mapper.  An input tuple has a key that is a long, the line in the input file,
	// and a value that is Text.  That value is processed by the verticesFromInputValue()
	// function, which returns a list of MRVertex instances.  A output tuple has a key
	// that is an int, the vertex index, and a value that is Text, the description of the
	// processed MRVertex.
	
	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, BytesWritable> {

		// By default, this function assumes that the Text value is what would be produced
		// by MRVertex.toText(MRVerte.FORMAT_EDGES_TO).  Derived classes can override this
		// function to take a Text value of a different format, as long as it is convertable
		// to one or more MRVertex instances.
		
		protected ArrayList<MRVertex> verticesFromInputValue(Text value) {
			ArrayList<MRVertex> result = new ArrayList<MRVertex>();
			result.add(new MRVertex(value));
			return result;
		}
				
		// The actual mapping function.
		
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			ArrayList<MRVertex> vertices = verticesFromInputValue(value);
			for (MRVertex vertex : vertices) {
				context.write(new IntWritable(vertex.getId()), 
							  vertex.toWritable(MRVertex.EdgeFormat.EDGES_TO));
				
				MRVertex.AdjacencyIterator itTo = vertex.createToAdjacencyIterator();
				for (int toId = itTo.begin(); !itTo.done(); toId = itTo.next()) {
					MREdge edge = new MREdge(vertex.getId(), toId);
					context.write(new IntWritable(toId), edge.toWritable());
				}
			}
		}
	}

	// The reducer.  The input tuple has a key that is an int, the index of a vertex,
	// and value that is Text, a description of a MRVertex.  The reducer merges all the
	// descriptions of the vertex with a particular index to collect its edges.  The
	// output tuple has a key that is an int, the index of the vertex, and a value that
	// is Text, the description of the processed MRVertex.
	
	public static class Reducer 
	extends org.apache.hadoop.mapreduce.Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
		
		protected MRVertex createMRVertex(BytesWritable value) {
			return new MRVertex(value);
		}
		
		protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			ArrayList<MREdge> edges = new ArrayList<MREdge>();
			MRVertex vertex = null;
			
			for (BytesWritable value : values) {
				if (MRVertex.getIsMRVertex(value)) {
					if (vertex == null) 
						vertex = createMRVertex(value);
					else 
						vertex.merge(createMRVertex(value));
				}
				else if (MREdge.getIsMREdge(value)) {
					edges.add(new MREdge(value));
				}
			}
			
			if (vertex != null) {
				for (MREdge edge : edges)
					vertex.addEdge(edge);
				
				if (coverage != DISABLE_COVERAGE_ERROR_CORRECTION) {
					boolean sufficientlyCoveredFrom = removeUndercoveredEdges(vertex, Which.FROM);
					boolean sufficientlyCoveredTo = removeUndercoveredEdges(vertex, Which.TO);
					
					if (!sufficientlyCoveredFrom && !sufficientlyCoveredTo)
						return;
				}
				
				vertex.computeIsBranch();
				vertex.computeIsSourceSink();
				
				// HEY!! 
				System.out.println("** " + vertex.toDisplayString() + " **");
				
				MRVertex.EdgeFormat format = includeFromEdges ? MRVertex.EdgeFormat.EDGES_TO_FROM : 
					MRVertex.EdgeFormat.EDGES_TO;
				BytesWritable value = vertex.toWritable(format);
				
				if (partitionBranchesChains) {
					if (MRVertex.getIsBranch(value))
						multipleOutputs.write(key, value, "branch/part");
					else
						multipleOutputs.write(key, value, "chain/part");					
				}
				else {
					context.write(key, value);
				}
			}
			
		}
		
		protected void setup(Context context)
	            throws IOException, InterruptedException {
			if (partitionBranchesChains)
				multipleOutputs = new MultipleOutputs<IntWritable, BytesWritable>(context);
		}

		protected void cleanup(Context context)
	            throws IOException, InterruptedException {
			if (partitionBranchesChains)
				multipleOutputs.close();
		}
		
		private enum Which { FROM, TO };
		
		private boolean removeUndercoveredEdges(MRVertex vertex, Which which) {
			int minCoverage = (int) Math.ceil(coverage / 2.0);
			
			MRVertex.AdjacencyMultipleIterator it = (which == Which.FROM) ? 
					vertex.createFromAdjacencyMultipleIterator() :
						vertex.createToAdjacencyMultipleIterator();
					
			int numSufficientlyCovered = 0;
			ArrayList<Integer> others = it.begin();
			while (!it.done()) {
				ArrayList<Integer> nexts = it.next();
				if (others.size() < minCoverage) {
					int other = others.get(0);
					for (int i = 0; i < others.size(); i++) {
						if (which == Which.FROM)
							vertex.removeEdgeFrom(other);
						else
							vertex.removeEdgeTo(other);
					}
				}
				else {
					numSufficientlyCovered++;
				}
				others = nexts;
			}
			
			return (numSufficientlyCovered > 0);
		}
		
		private MultipleOutputs<IntWritable, BytesWritable> multipleOutputs = null;

	}
	
    private static boolean partitionBranchesChains = false;
    private static boolean includeFromEdges = false;
    private static int coverage = DISABLE_COVERAGE_ERROR_CORRECTION;
    
}
