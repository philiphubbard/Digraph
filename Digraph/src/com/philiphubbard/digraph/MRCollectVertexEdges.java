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

import com.philiphubbard.digraph.MRVertex;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// A mapper and reducer for a Hadoop map-reduce algorithm to collect the edges associated
// with a vertex, turning vertex descriptions with only the edges pointing out from the
// vertex into vertex descriptions that also include the edges pointing in from other
// vertices.

public class MRCollectVertexEdges {

	// TODO: Consider switching from int to long for vertex identifiers.
	
	// The mapper.  An input tuple has a key that is a long, the line in the input file,
	// and a value that is Text.  That value is processed by the verticesFromInputValue()
	// function, which returns a list of MRVertex instances.  A output tuple has a key
	// that is an int, the vertex index, and a value that is Text, the description of the
	// processed MRVertex.
	
	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {

		// By default, this function assumes that the Text value is what would be produced
		// by MRVertex.toText(MRVerte.FORMAT_TO_EDGES).  Derived classes can override this
		// function to take a Text value of a different format, as long as it is convertable
		// to one or more MRVertex instances.
		
		protected ArrayList<MRVertex> verticesFromInputValue(Text value) {
			ArrayList<MRVertex> result = new ArrayList<MRVertex>();
			result.add(new MRVertex(value));
			return result;
		}
		
		// The actual mapping function.
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			ArrayList<MRVertex> vertices = verticesFromInputValue(value);
			for (MRVertex vertex : vertices) {
				context.write(new IntWritable(vertex.id()), 
							  vertex.toText(MRVertex.FORMAT_TO_EDGES));
				
				MRVertex.AdjacencyIterator itTo = vertex.createToAdjacencyIterator();
				for (int toId = itTo.begin(); !itTo.done(); toId = itTo.next()) {
					MRVertex to = new MRVertex(toId);
					to.addEdgeFrom(vertex.id());
					
					context.write(new IntWritable(toId), 
								  to.toText(MRVertex.FORMAT_TO_FROM_EDGES));
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
	extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			MRVertex vertex = new MRVertex(key.get());
			for (Text text : values) {
				MRVertex other = new MRVertex(text);
				vertex.merge(other);
			}
			
			context.write(key, vertex.toText(MRVertex.FORMAT_TO_FROM_EDGES));
		}
	}

}
