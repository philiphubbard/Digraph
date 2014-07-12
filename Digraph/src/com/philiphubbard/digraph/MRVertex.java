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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException; 
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
		this(text.toString());
	}
	
	public MRVertex(String s) {
		edges = new EdgeLink[2];
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;

		/* HEY!! Disabled for testing
		if (!getIsMRVertex(text))
			return;
			*/
		
		// HEY!!
		//char header = s.charAt(0);
		s = s.substring(1);
		// HEY!! If not writing "from" edges, get the header flag about being a branch or not?
		
		// Five parts: header and ID; format; edges to other vertices; edges from other vertices;
		// subclass data.
		
		String[] tokens = s.split(SEPARATOR, 5);
		
		// TODO: Add error handling for malformed strings.
		int iToken = 0;
		id = Integer.parseInt(tokens[iToken++]);
		
		String edgeFormat = tokens[iToken++];
		
		String edgesTo = tokens[iToken++];
		if (!edgesTo.isEmpty()) {
			for (String edge : edgesTo.split(EDGE_SEPARATOR))
				addEdgeTo(Integer.parseInt(edge));
		}
		
		if (edgeFormat.equals(FORMAT_EDGES_TO_FROM)) {
			String edgesFrom = tokens[iToken++];
			if (!edgesFrom.isEmpty()) {
				for (String edge : edgesFrom.split(EDGE_SEPARATOR))
					addEdgeFrom(Integer.parseInt(edge));
			}
		}
		
		if (!tokens[iToken].isEmpty())
			fromTextInternal(tokens[iToken]);
	}
	
	// The vertex's identifier.
	
	public int getId() {
		return id;
	}
	
	// Write this vertex to a Hadoop Text (Writable) instance.
	// The format argument specifies whether to write only the edges
	// pointing to other vertices from this vertex, or to write both
	// those edges and the edges pointing from other vertices to this
	// vertex.

	public enum EdgeFormat { EDGES_TO, EDGES_TO_FROM };
	
	public Text toText(EdgeFormat edgeFormat) {
		return toText(edgeFormat, TextFormat.VALUE);
	}
	
	public enum TextFormat { VALUE, KEY_VALUE }
	
	public Text toText(EdgeFormat edgeFormat, TextFormat textFormat) {
		StringBuilder s = new StringBuilder();
		
		if (textFormat == TextFormat.KEY_VALUE) {
			s.append(id);
			s.append("\t");
		}
		
		setHeader(s);
		s.append(id);
		s.append(SEPARATOR);
		s.append((edgeFormat == EdgeFormat.EDGES_TO) ? FORMAT_EDGES_TO : 
			FORMAT_EDGES_TO_FROM);
		s.append(SEPARATOR);
		EdgeLink link = edges[INDEX_EDGES_TO];
		while (link != null) {
			s.append(link.vertex);
			if (link.next != null)
				s.append(EDGE_SEPARATOR);
			link = link.next;
		}
		if (edgeFormat == EdgeFormat.EDGES_TO_FROM) {
			s.append(SEPARATOR);
			link = edges[INDEX_EDGES_FROM];
			while (link != null) {
				s.append(link.vertex);
				if (link.next != null)
					s.append(EDGE_SEPARATOR);
				link = link.next;
			}
		}
		
		s.append(SEPARATOR);
		toTextInternal(s);
		
		if (textFormat == TextFormat.KEY_VALUE)
			s.append("\n");
		
		return new Text(s.toString());
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
			return getId();
		
		// HEY!! With Java 1.7, can use java.util.concurrent.ThreadLocalRandom?
		int r = 0;
		long t = System.nanoTime();
		t += getId();
		for (int i = 0; i < 8; i++)
			r += ((t >>= 1) & 0x1);
		
		// HEY!! Debug
		System.out.println("*** t " + t + " r = " + r + " ***");
		
		if (r % 2 == 0)
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
		int otherTo = other.getTail();
		if (otherTo == NO_VERTEX)
			return;
		
		mergeInternal(other);
		
		clearEdges();
		addEdgeTo(otherTo);
	}
	
	//
	
	public boolean equals(MRVertex other) {
		if (id != other.id)
			return false;
		
		HashSet<Integer> toSet = new HashSet<Integer>();
		MRVertex.AdjacencyIterator toIt = createToAdjacencyIterator();
		for (int to = toIt.begin(); !toIt.done(); to = toIt.next())
			toSet.add(to);
		HashSet<Integer> toSetOther = new HashSet<Integer>();
		MRVertex.AdjacencyIterator toItOther = other.createToAdjacencyIterator();
		for (int to = toItOther.begin(); !toItOther.done(); to = toItOther.next())
			toSetOther.add(to);
		if (!toSet.equals(toSetOther))
			return false;
		
		HashSet<Integer> fromSet = new HashSet<Integer>();
		MRVertex.AdjacencyIterator fromIt = createFromAdjacencyIterator();
		for (int from = fromIt.begin(); !fromIt.done(); from = fromIt.next())
			fromSet.add(from);
		HashSet<Integer> fromSetOther = new HashSet<Integer>();
		MRVertex.AdjacencyIterator fromItOther = other.createFromAdjacencyIterator();
		for (int from = fromItOther.begin(); !fromItOther.done(); from = fromItOther.next())
			fromSetOther.add(from);
		if (!fromSet.equals(fromSetOther))
			return false;
		
		return true;
	}
	
	public String toDisplayString() {
		StringBuilder s = new StringBuilder();
		
		s.append("MRVertex ");
		s.append(getId());
		
		MRVertex.AdjacencyIterator toIt = createToAdjacencyIterator();
		if (toIt.begin() != NO_VERTEX) {
			s.append(": to: ");
			for (int to = toIt.begin(); !toIt.done(); to = toIt.next()) {
				s.append(to);
				s.append(" ");
			}
		}
		
		MRVertex.AdjacencyIterator fromIt = createFromAdjacencyIterator();
		if (fromIt.begin() != NO_VERTEX) {
			s.append(": from: ");
			for (int from = fromIt.begin(); !fromIt.done(); from = fromIt.next()) {
				s.append(from);
				s.append(" ");
			}
		}
		
		return s.toString();
	}
	
	static void read(FSDataInputStream in, ArrayList<MRVertex> vertices) throws IOException {
		InputStreamReader inReader = new InputStreamReader(in, Charset.forName("UTF-8"));
		BufferedReader bufferedReader = new BufferedReader(inReader);
		String line;
		while((line = bufferedReader.readLine()) != null) {
			String[] tokens = line.split("\t", 2);
			MRVertex vertex = new MRVertex(tokens[1]);
			vertices.add(vertex);
		}
	}
	
	static void write(FSDataOutputStream out, ArrayList<MRVertex> vertices) throws IOException {
		for (MRVertex vertex : vertices) {
			Text text = vertex.toText(MRVertex.EdgeFormat.EDGES_TO, MRVertex.TextFormat.KEY_VALUE);
			byte[] bytes = text.copyBytes();
			for (byte b : bytes)
				out.write(b);
		}
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
		/* HEY!! Old, does it not work?
		AdjacencyIterator itTo = createToAdjacencyIterator();
		int to = itTo.begin();
		if (itTo.done())
			return NO_VERTEX;
		itTo.next();
		if (!itTo.done())
			return NO_VERTEX;
		return to;
			*/
		if ((edges[INDEX_EDGES_TO] != null) && (edges[INDEX_EDGES_TO].next == null))
			return edges[INDEX_EDGES_TO].vertex;
		else
			return NO_VERTEX;
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
	
	// HEY!! Private or protected?
	
	private static final short NUM_TYPE_BITS = 8;
	private static final short TEXT_TYPE_ID = 1;
	
	private static final int FLAGS_MASK = 0xFF;
	private static final short FLAG_IS_BRANCH = 0x1;
	
	private static final String FORMAT_EDGES_TO = "t";
	private static final String FORMAT_EDGES_TO_FROM = "b";

	protected static final String SEPARATOR = ";";
	protected static final String EDGE_SEPARATOR = ",";
	
	private int id;
	
	private static boolean allowEdgeMultiples = true;
	
	private static final int INDEX_EDGES_TO = 0;
	private static final int INDEX_EDGES_FROM = 1;
	private EdgeLink[] edges;
}
