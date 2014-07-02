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
	
	public static boolean getIsMRVertex(Text text) {
		int header = text.charAt(0);
		short type = (short) (header >>> (16 - NUM_TYPE_BITS));
		return (type == TEXT_TYPE_ID);
	}
	
	public static boolean getIsBranch(Text text) {
		return ((getFlags(text) & FLAG_IS_BRANCH) != 0);
	}
	
	// A special identifier representing no vertex.
	
	static final int NO_VERTEX = -1;
	
	public static boolean getAllowEdgeMultiples() {
		return allowEdgeMultiples;
	}
	
	public static void setAllowEdgeMultiples(boolean doAllow) {
		allowEdgeMultiples = doAllow;
	}
	
	// Constructor, specifying the identifier for this vertex.
	
	public MRVertex(int id) {
		this.id = id;
		edges = new EdgeLink[2];
		clearEdges();
	}
	
	// Constructor, building the vertex from a Hadoop Text (Writable) instance.
	
	public MRVertex(Text text) {
		edges = new EdgeLink[2];
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;

		/* HEY!! Disabled for testing
		if (!getIsMRVertex(text))
			return;
			*/
		
		String s = text.toString();
		// HEY!!
		//char header = s.charAt(0);
		s = s.substring(1);
		// HEY!! If not writing "from" edges, get the header flag about being a branch or not?
		
		String[] tokens = s.split(SEPARATOR);
		// TODO: Add error handling for malformed strings.
		id = Integer.parseInt(tokens[0]);
		String format = tokens[1];
		int i;
		for (i = 2; i < tokens.length; i++) {
			int j = Integer.parseInt(tokens[i]);
			if ((format.equals(FORMAT_EDGES_TO_FROM)) && (j == NO_VERTEX))
				break;
			addEdgeTo(j);
		}
		if (format.equals(FORMAT_EDGES_TO_FROM)) {
			for (i++; i < tokens.length; i++) {
				int j = Integer.parseInt(tokens[i]);
				addEdgeFrom(j);
			}
		}
		
		int i2 = s.lastIndexOf(SEPARATOR);
		if (i2 < s.length() - 1) {
			String s2 = s.substring(i2 + 1);
			fromTextInternal(s2);
		}
	}
	
	// Write this vertex to a Hadoop Text (Writable) instance.
	// The format argument specifies whether to write only the edges
	// pointing to other vertices from this vertex, or to write both
	// those edges and the edges pointing from other vertices to this
	// vertex.

	public static final String FORMAT_EDGES_TO = "t";
	public static final String FORMAT_EDGES_TO_FROM = "b";
	
	public Text toText(String format) {
		StringBuilder s = new StringBuilder();
		setHeader(s);
		s.append(id);
		s.append(SEPARATOR);
		s.append(format);
		s.append(SEPARATOR);
		EdgeLink link = edges[INDEX_EDGES_TO];
		while (link != null) {
			s.append(link.vertex);
			s.append(SEPARATOR);
			link = link.next;
		}
		if (format.equals(FORMAT_EDGES_TO_FROM)) {
			s.append(NO_VERTEX);
			s.append(SEPARATOR);
			link = edges[INDEX_EDGES_FROM];
			while (link != null) {
				s.append(link.vertex);
				s.append(SEPARATOR);
				link = link.next;
			}
		}
		
		s.append(SEPARATOR);
		toTextInternal(s);
		
		return new Text(s.toString());
	}
	
	// The vertex's identifier.
	
	public int getId() {
		return id;
	}
	
	// Add an edge that points to the specified vertex from this vertex.
	
	public void addEdgeTo(int to) {
		addEdge(to, INDEX_EDGES_TO);
	}
	
	// Add an edge that points from the specified vertex to this vertex.
	
	public void addEdgeFrom(int from) {
		addEdge(from, INDEX_EDGES_FROM);
	}
	
	public void addEdge(MREdge edge) {
		if (edge.getTo() == getId())
			addEdgeFrom(edge.getFrom());
		else if (edge.getFrom() == getId())
			addEdgeTo(edge.getTo());
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
		return new AdjacencyIterator(edges[INDEX_EDGES_FROM]);
	}
	
	// Create an iterator over other the edges pointing to other vertices
	// from this vertex.
	
	public AdjacencyIterator createToAdjacencyIterator() {
		return new AdjacencyIterator(edges[INDEX_EDGES_TO]);
	}
	
	//
	
	public int getMergeKey() {
		int to = getTail();
		if (to == NO_VERTEX)
			return NO_VERTEX;
		if (System.nanoTime() % 2 == 0)
			return to;
		else
			return getId();
	}
	
	public static MRVertex merge(MRVertex v1, MRVertex v2, int key) {
		if (key != NO_VERTEX) {
			if (key == v1.getId()) {
				v2.merge(v1);
				return v2;
			}
			else if (key == v2.getId()) {
				v1.merge(v2);
				return v1;
			}
		}
		return new MRVertex(NO_VERTEX);
	}
	
	// Note that after merging, it does not make sense to call toText()
	// with FORMAT_EDGES_TO_FROM because there will be invalid data.
	// E.g., if v1->v2, v2->v3, after merging v1 and v2, v3 will still
	// record that it has an edge from v2. 
	
	public void merge(MRVertex other) {
		int to = getTail();
		if (to != other.getId())
			return;
		
		mergeInternal(other);
		
		int otherTo = other.getTail();
		clearEdges();
		addEdgeTo(otherTo);
	}
	
	//
	
	protected static short getFlags(Text text) {
		int header = text.charAt(0);
		return (short) (header & FLAGS_MASK);
	}
	
	protected void setHeader(StringBuilder s) {
		char header = (char) (TEXT_TYPE_ID << (16 - NUM_TYPE_BITS));
		
		boolean isBranch = false;
		if ((edges[INDEX_EDGES_TO] != null) && (edges[INDEX_EDGES_TO].next != null))
			isBranch = true;
		else if ((edges[INDEX_EDGES_FROM] != null) && (edges[INDEX_EDGES_FROM].next != null))
			isBranch = true;
		if (isBranch) {
			header |= FLAG_IS_BRANCH;
		}
		
		s.append(header);
	}
	
	protected int getTail() {
		AdjacencyIterator itTo = createToAdjacencyIterator();
		int to = itTo.begin();
		if (itTo.done())
			return NO_VERTEX;
		itTo.next();
		if (!itTo.done())
			return NO_VERTEX;
		return to;
	}
	
	protected void mergeInternal(MRVertex other) {
	}
	
	protected void toTextInternal(StringBuilder s) {
	}
	
	protected void fromTextInternal(String s) {
	}
	
	//
	
	private void clearEdges() {
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;		
	}
	
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
	
	private static final short NUM_TYPE_BITS = 8;
	private static final short TEXT_TYPE_ID = 1;
	
	private static final int FLAGS_MASK = 0xFF;
	private static final short FLAG_IS_BRANCH = 0x1;
	
	private static final String SEPARATOR = ",";
	
	private int id;
	
	private static boolean allowEdgeMultiples = true;
	
	private static final int INDEX_EDGES_TO = 0;
	private static final int INDEX_EDGES_FROM = 1;
	private EdgeLink[] edges;
}
