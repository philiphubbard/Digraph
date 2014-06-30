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

import org.apache.hadoop.io.Text;

// A directed-graph vertex for use with Hadoop map-reduce algorithms.
// To support the distributed nature of map-reduce algorithms, this vertex
// is not part of a global graph (like a Digraph<E> instance) but instead
// keeps its own record of its adjacent vertices, and can read and write
// this information from and to a Hadoop Writable instance.

public class MRVertex {
	
	// A special identifier representing no vertex.
	
	static final int NO_VERTEX = -1;
	
	public static boolean getAllowEdgeMultiples() {
		return allowEdgeMultiples;
	}
	
	public static void setAllowEdgeMultiples(boolean allow) {
		allowEdgeMultiples = allow;
	}
	
	// Constructor, specifying the identifier for this vertex.
	
	public MRVertex(int id) {
		this.id = id;
		edges = new EdgeLink[2];
		edges[INDEX_TO_EDGES] = null;
		edges[INDEX_FROM_EDGES] = null;
	}
	
	// Constructor, building the vertex from a Hadoop Text (Writable) instance.
	
	public MRVertex(Text text) {
		edges = new EdgeLink[2];
		edges[INDEX_TO_EDGES] = null;
		edges[INDEX_FROM_EDGES] = null;

		String s = text.toString();
		String[] tokens = s.split(SEPARATOR);
		// TODO: Add error handling for malformed strings.
		id = Integer.parseInt(tokens[0]);
		String format = tokens[1];
		int i;
		for (i = 2; i < tokens.length; i++) {
			int j = Integer.parseInt(tokens[i]);
			if ((format.equals(FORMAT_TO_FROM_EDGES)) && (j == NO_VERTEX))
				break;
			addEdgeTo(j);
		}
		if (format.equals(FORMAT_TO_FROM_EDGES)) {
			for (i++; i < tokens.length; i++) {
				int j = Integer.parseInt(tokens[i]);
				addEdgeFrom(j);
			}
		}
	}
	
	// Write this vertex to a Hadoop Text (Writable) instance.
	// The format argument specifies whether to write only the edges
	// pointing to other vertices from this vertex, or to write both
	// those edges and the edges pointing from other vertices to this
	// vertex.

	public static final String FORMAT_TO_EDGES = "t";
	public static final String FORMAT_TO_FROM_EDGES = "b";
	
	public Text toText(String format) {
		StringBuilder s = new StringBuilder();
		s.append(id);
		s.append(SEPARATOR);
		s.append(format);
		s.append(SEPARATOR);
		EdgeLink link = edges[INDEX_TO_EDGES];
		while (link != null) {
			s.append(link.vertex);
			s.append(SEPARATOR);
			link = link.next;
		}
		if (format.equals(FORMAT_TO_FROM_EDGES)) {
			s.append(NO_VERTEX);
			s.append(SEPARATOR);
			link = edges[INDEX_FROM_EDGES];
			while (link != null) {
				s.append(link.vertex);
				s.append(SEPARATOR);
				link = link.next;
			}
		}
		return new Text(s.toString());
	}
	
	// The vertex's identifier.
	
	public int id() {
		return id;
	}
	
	// Add an edge that points to the specified vertex from this vertex.
	
	public void addEdgeTo(int to) {
		addEdge(to, INDEX_TO_EDGES);
	}
	
	// Add an edge that points from the specified vertex to this vertex.
	
	public void addEdgeFrom(int from) {
		addEdge(from, INDEX_FROM_EDGES);
	}
	
	// An iterator over vertices adjacent to a MRVertex.
	// An iterator is created by the createFromAdjacencyIterator() or
	// createToAdjacencyIterator() functions, below.
	// Then iteration can be performed with a loop like the following:
	// "for (int v = iterator.begin(); !iterator.done(); v = iterator.next())"
	
	public class AdjacencyIterator {
		
		public int begin() {
			current = edges;
			return (current != null) ? current.vertex : NO_VERTEX;
		}
		
		public int next() {
			if (current != null)
				current = current.next;
			return (current != null) ? current.vertex : NO_VERTEX;
		}
		
		public boolean done() {
			return (current == null);
		}
		
		private AdjacencyIterator(EdgeLink edges) {
			this.edges = edges;
			current = null;
		}
		
		private EdgeLink current;
		private EdgeLink edges;
		
	}
	
	// Create an iterator over the edges pointing from other vertices
	// to this vertex.
	
	public AdjacencyIterator createFromAdjacencyIterator() {
		return new AdjacencyIterator(edges[INDEX_FROM_EDGES]);
	}
	
	// Create an iterator over other the edges pointing to other vertices
	// from this vertex.
	
	public AdjacencyIterator createToAdjacencyIterator() {
		return new AdjacencyIterator(edges[INDEX_TO_EDGES]);
	}
	
	// Map-reduce algorithms can involve a reducer that receives several
	// versions of a vertex, each with pieces of data for that vertex.
	// This routine will merge two such pieces together.
	// Does nothing if the vertices do not have matching identifiers.
	
	public void merge(MRVertex other) {
		if (other.id() != id)
			return;
		AdjacencyIterator itTo = other.createToAdjacencyIterator();
		for (int v = itTo.begin(); !itTo.done(); v = itTo.next())
			addEdgeTo(v);
		AdjacencyIterator itFrom = other.createFromAdjacencyIterator();
		for (int v = itFrom.begin(); !itFrom.done(); v = itFrom.next())
			addEdgeFrom(v);
	}
	
	//
	
	private void addEdge(int vertex, int which) {
		if (allowEdgeMultiples) {
			edges[which] = new EdgeLink(vertex, edges[which]);
		}
		else {
			// Keep edges sorted to improve average-case performance.
			EdgeLink link = edges[which];
			EdgeLink prev = null;
			while (link != null) {
				if (vertex < link.vertex)
					break;
				else if (vertex == link.vertex)
					return;
				prev = link;
				link = link.next;
			}
			if (prev == null)
				edges[which] = new EdgeLink(vertex, edges[which]);
			else
				prev.next = new EdgeLink(vertex, link);
		}		
	}
	
	private class EdgeLink {
		EdgeLink(int vertex, EdgeLink next) {
			this.vertex = vertex;
			this.next = next;
		}
		int vertex;
		EdgeLink next;
	}
	
	private static final String SEPARATOR = ",";
	private static boolean allowEdgeMultiples = true;
	private int id;
	private static final int INDEX_TO_EDGES = 0;
	private static final int INDEX_FROM_EDGES = 1;
	private EdgeLink[] edges;
}
