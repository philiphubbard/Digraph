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

import java.lang.ref.WeakReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;

// A directed-graph vertex for use with Hadoop map-reduce algorithms.
// To support the distributed nature of map-reduce algorithms, this vertex
// is not part of a global graph (like a Digraph<E> instance) but instead
// keeps its own record of its adjacent vertices, and can read and write
// this information from and to a Hadoop Writable instance.

public class MRVertex {
	
	public static final String CONFIG_ALLOW_EDGE_MULTIPLES = "ALLOW_EDGE_MULTIPLES";
	
	public static final String CONFIG_COMPRESS_CHAIN_MULTIPLES_MUST_MATCH = 
			"CONFIG_COMPRESS_CHAIN_MULTIPLES_MUST_MATCH";
	
	// Constructor, specifying the identifier for this vertex.
	
	public MRVertex(int id, Configuration config) {
		this.id = id;
		this.config = config;
		flags = (byte) 0;
		edges = new EdgeLink[2];
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;
		iterators = new ArrayList<WeakReference<EdgeHolder>>();
	}
	
	// Constructor, building the vertex from a Hadoop Writable instance.
	// If the BytesWritable is truncated, this constructor reads and
	// constructs as much as much as possible, without indicating an error.
	
	public MRVertex(BytesWritable writable, Configuration config) {
		this.config = config;
		
		byte [] array = writable.getBytes();
		
		flags = array[1];
		
		int i = 2;
		id = getInt(array, i);
		i += 4;
		
		edges = new EdgeLink[2];
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;

		short numEdgesTo = getShort(array, i);
		i += 2;
		for (int j = 0; j < numEdgesTo; j++) {
			addEdgeTo(getInt(array, i));
			i += 4;
		}
		
		short numEdgesFrom = getShort(array, i);
		i += 2;
		for (int j = 0; j < numEdgesFrom; j++) {
			addEdgeFrom(getInt(array, i));
			i += 4;
		}
		
		short numBytesInternal = getShort(array, i);
		i += 2;
		if (numBytesInternal != 0)
			fromWritableInternal(array, i, numBytesInternal);

		iterators = new ArrayList<WeakReference<EdgeHolder>>();
	}
	
	// The vertex's identifier.
	
	public int getId() {
		return id;
	}
	
	// A special identifier representing no vertex.
	
	public static final int NO_VERTEX = -1;
	
	// Write this vertex to a Hadoop Text (Writable) instance.
	// The format argument specifies whether to write only the edges
	// pointing to other vertices from this vertex, or to write both
	// those edges and the edges pointing from other vertices to this
	// vertex.

	public enum EdgeFormat { EDGES_TO, EDGES_TO_FROM };
	
	public enum WritableFormat { VALUE, VALUE_LINE, KEY_VALUE_LINE }
	
	public BytesWritable toWritable(EdgeFormat edgeFormat) throws IOException {
		short numEdgesTo = 0;
		for (EdgeLink link = edges[INDEX_EDGES_TO]; link != null; link = link.next)
			numEdgesTo++;
		
		short numEdgesFrom = 0;
		if (edgeFormat == EdgeFormat.EDGES_TO_FROM) {
			for (EdgeLink link = edges[INDEX_EDGES_FROM]; link != null; link = link.next)
				numEdgesFrom++;
		}
		
		byte[] internal = toWritableInternal();
		
		// Header + id + numEdgesTo + edges to + numEdgesFrom + edges from.
		int numBytes = 2 + 4 + 2 + 4 * numEdgesTo  + 2 + 4 * numEdgesFrom + 2;
		if (internal != null) 
			numBytes += internal.length;
		byte[] result = new byte[numBytes];

		result[0] = WRITABLE_TYPE_ID;
		result[1] = flags;

		int i = 2;
		i = putInt(id, result, i);
		
		i = putShort(numEdgesTo, result, i);
		for (EdgeLink link = edges[INDEX_EDGES_TO]; link != null; link = link.next)
			i = putInt(link.vertex, result, i);
		
		i = putShort(numEdgesFrom, result, i);
		if (edgeFormat == EdgeFormat.EDGES_TO_FROM) {
			for (EdgeLink link = edges[INDEX_EDGES_FROM]; link != null; link = link.next) 
				i = putInt(link.vertex, result, i);
		}
			
		if (internal != null) {
			if (internal.length > Short.MAX_VALUE)
				throw new IOException("MRVertex " + getId() + " toWritableInternal() is too long");

			i = putShort((short) internal.length, result, i);
			for (byte b : internal)
				result[i++] = b;
		}
		else {
			i = putShort((short) 0, result, i);
		}
		
		return new BytesWritable(result);
	}
	
	//
	
	// Add an edge that points to the specified vertex from this vertex.
	
	public void addEdgeTo(int to) {
		addEdge(to, INDEX_EDGES_TO);
		
		// Recompute the branch status with the new edge only if this vertex
		// was not already a branch, since the previous conclusion that this vertex
		// was a branch may have been based on edges from other vertices that are
		// not stored with this vertex.
		
		if (!getIsBranch())
			computeIsBranch();
		
		if (getIsSink())
			computeIsSourceSink();
	}
	
	// Add an edge that points from the specified vertex to this vertex.
	
	public void addEdgeFrom(int from) {
		addEdge(from, INDEX_EDGES_FROM);
		
		// See the comment in addEdgeTo().
		
		if (!getIsBranch())
			computeIsBranch();
		
		if (getIsSource())
			computeIsSourceSink();
	}
	
	// Remove an edge that points to the specified vertex from this vertex.
	
	public void removeEdgeTo(int to) {
		removeEdge(to, INDEX_EDGES_TO);
		
		// See the comment in addEdgeTo().
		
		if (!getIsBranch())
			computeIsBranch();
		
		if (getIsSink())
			computeIsSourceSink();
	}
	
	// Remove an edge that points from the specified vertex to this vertex.
	
	public void removeEdgeFrom(int from) {
		removeEdge(from, INDEX_EDGES_FROM);
		
		// See the comment in addEdgeTo().
		
		if (!getIsBranch())
			computeIsBranch();
		
		if (getIsSource())
			computeIsSourceSink();
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
	
	public class AdjacencyIterator extends EdgeHolder {
		
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
		
		private EdgeLink edges;
		
	}
	
	// Create an iterator over the edges pointing from other vertices
	// to this vertex.
	
	public AdjacencyIterator createFromAdjacencyIterator() {
		AdjacencyIterator it = new AdjacencyIterator(edges[INDEX_EDGES_FROM]);
		
		// Since there is no automatic removal of weak references to iterators
		// that have become null, now is a reasonable time to try explicit removal.

		cleanupIterators();
		
		iterators.add(new WeakReference<EdgeHolder>(it));
		return it;
	}
	
	// Create an iterator over other the edges pointing to other vertices
	// from this vertex.
	
	public AdjacencyIterator createToAdjacencyIterator() {
		AdjacencyIterator it = new AdjacencyIterator(edges[INDEX_EDGES_TO]);
		
		// Since there is no automatic removal of weak references to iterators
		// that have become null, now is a reasonable time to try explicit removal.

		cleanupIterators();
		
		iterators.add(new WeakReference<EdgeHolder>(it));
		return it;
	}
	
	// An iterator over vertices adjacent to a MRVertex, returning multiple
	// instances of a vertex connected with multiple edges.
	// An iterator is created by the createFromAdjacencyIterator() or
	// createToAdjacencyIterator() functions, below.
	// Then iteration can be performed with a loop like the following:
	// "for (int v = iterator.begin(); !iterator.done(); v = iterator.next())"
	
	public class AdjacencyMultipleIterator extends EdgeHolder {
		
		public ArrayList<Integer> begin() {
			current = edges;
			return matchingEdges();
		}
		
		public ArrayList<Integer> next() {
			if (current != null)
				current = current.next;
			return matchingEdges();
		}
		
		public boolean done() {
			return (current == null);
		}
		
		private AdjacencyMultipleIterator(EdgeLink edges) {
			this.edges = edges;
			current = null;
		}
		
		private ArrayList<Integer> matchingEdges() {
			if (current == null)
				return null;
			ArrayList<Integer> result = new ArrayList<Integer>();
			EdgeLink next = current;
			do {
				current = next;
				result.add(current.vertex);
				next = current.next;
			} while ((next != null) && (next.vertex == current.vertex));
			return result;			
		}
		
		private EdgeLink edges;
		
	}
	
	// Create an iterator over the edges pointing from other vertices
	// to this vertex.
	
	public AdjacencyMultipleIterator createFromAdjacencyMultipleIterator() {
		AdjacencyMultipleIterator it = new AdjacencyMultipleIterator(edges[INDEX_EDGES_FROM]);
		
		// Since there is no automatic removal of weak references to iterators
		// that have become null, now is a reasonable time to try explicit removal.

		cleanupIterators();
		
		iterators.add(new WeakReference<EdgeHolder>(it));
		return it;
	}
	
	// Create an iterator over other the edges pointing to other vertices
	// from this vertex.
	
	public AdjacencyMultipleIterator createToAdjacencyMultipleIterator() {
		AdjacencyMultipleIterator it = new AdjacencyMultipleIterator(edges[INDEX_EDGES_TO]);
		
		// Since there is no automatic removal of weak references to iterators
		// that have become null, now is a reasonable time to try explicit removal.

		cleanupIterators();
		
		iterators.add(new WeakReference<EdgeHolder>(it));
		return it;
	}
	
	//
	
	// The compute...() routines must be called for the get...() routines to be accurate.
	// Even then, they may not stay accurate, given the distributed nature of the graph.
	
	public void computeIsBranch() {
		flags &= ~FLAG_IS_BRANCH;
		
		int vertex = NO_VERTEX;
		for (EdgeLink edge = edges[INDEX_EDGES_TO]; edge != null; edge = edge.next) {
			if (vertex == NO_VERTEX) {
				vertex = edge.vertex;
			}
			else if (vertex != edge.vertex) {
				flags |= FLAG_IS_BRANCH;
				return;
			}
		}
		
		vertex = NO_VERTEX;
		for (EdgeLink edge = edges[INDEX_EDGES_FROM]; edge != null; edge = edge.next) {
			if (vertex == NO_VERTEX) {
				vertex = edge.vertex;
			}
			else if (vertex != edge.vertex) {
				flags |= FLAG_IS_BRANCH;
				return;
			}
		}
	}
	
	public void computeIsSourceSink() {
		short numEdgesToOthers = 0;
		for (EdgeLink link = edges[INDEX_EDGES_TO]; link != null; link = link.next)
			numEdgesToOthers++;
		
		short numEdgesFromOthers = 0;
		for (EdgeLink link = edges[INDEX_EDGES_FROM]; link != null; link = link.next)
			numEdgesFromOthers++;
		
		flags &= ~FLAG_IS_SOURCE;
		flags &= ~FLAG_IS_SINK;
		
		if ((numEdgesToOthers > 0) && (numEdgesFromOthers == 0))
			flags |= FLAG_IS_SOURCE;
		if ((numEdgesToOthers == 0) && (numEdgesFromOthers > 0))
			flags |= FLAG_IS_SINK;
	}
	
	public boolean getIsBranch() {
		return ((flags & FLAG_IS_BRANCH) != 0);
	}

	public boolean getIsSource() {
		return ((flags & FLAG_IS_SOURCE) != 0);
	}

	public boolean getIsSink() {
		return ((flags & FLAG_IS_SINK) != 0);
	}

	public static boolean getIsMRVertex(BytesWritable writable) {
		byte[] array = writable.getBytes();
		return (array[0] == WRITABLE_TYPE_ID);
	}
	
	public static boolean getIsBranch(BytesWritable writable) {
		return ((getFlags(writable) & FLAG_IS_BRANCH) != 0);
	}
	
	public static boolean getIsSource(BytesWritable writable) {
		return ((getFlags(writable) & FLAG_IS_SOURCE) != 0);
	}
	
	public static boolean getIsSink(BytesWritable writable) {
		return ((getFlags(writable) & FLAG_IS_SINK) != 0);
	}
	
	//
	
	public int getCompressChainKey(Random random) {
		Tail tail = getTail();
		int to = tail.id;
		if (to == NO_VERTEX)
			return getId();
		return random.nextBoolean() ? to : getId();
	}
	
	public static MRVertex compressChain(MRVertex v1, MRVertex v2, int key,
			Configuration config) {
		if (key != NO_VERTEX) {
			if (key == v1.getId()) {
				if (!v2.compressChain(v1, config))
					return null;
				else
					return v2;
			}
			else if (key == v2.getId()) {
				if (!v1.compressChain(v2, config))
					return null;
				else
					return v1;
			}
		}
		return null;
	}
	
	// Note that after merging, it does not make sense to call toText()
	// with FORMAT_EDGES_TO_FROM because there will be invalid data.
	// E.g., if v1->v2, v2->v3, after merging v1 and v2, v3 will still
	// record that it has an edge from v2. 
	
	public boolean compressChain(MRVertex other, Configuration config) {
		Tail tail = getTail();
		if (tail.id != other.getId())
			return false;
		Tail otherTail = other.getTail();
		if (otherTail.id == NO_VERTEX)
			return false;
		
		boolean multiplesMustMatch = 
				config.getBoolean(CONFIG_COMPRESS_CHAIN_MULTIPLES_MUST_MATCH, false);
		if (multiplesMustMatch && (tail.count != otherTail.count))
			return false;
		
		int count = (tail.count < otherTail.count) ? tail.count : otherTail.count;
		
		compressChainInternal(other, config);
		
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;		
		
		for (int i = 0; i < count; i++)
			addEdge(otherTail.id, INDEX_EDGES_TO);
		
		return true;
	}
	
	public void merge(MRVertex other) {
		if (other.getId() == getId()) {
			AdjacencyIterator itTo = other.createToAdjacencyIterator();
			for (int to = itTo.begin(); !itTo.done(); to = itTo.next()) 
				addEdgeTo(to);
			AdjacencyIterator itFrom = other.createFromAdjacencyIterator();
			for (int from = itFrom.begin(); !itFrom.done(); from = itFrom.next()) 
				addEdgeFrom(from);
		}
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
			s.append("; to:");
			for (int to = toIt.begin(); !toIt.done(); to = toIt.next()) {
				s.append(" ");
				s.append(to);
			}
		}
		
		MRVertex.AdjacencyIterator fromIt = createFromAdjacencyIterator();
		if (fromIt.begin() != NO_VERTEX) {
			s.append("; from:");
			for (int from = fromIt.begin(); !fromIt.done(); from = fromIt.next()) {
				s.append(" ");
				s.append(from);
			}
		}
		
		return s.toString();
	}
	
	//
	
	// Text routines are for debugging only.
	
	public MRVertex(Text text, Configuration config) {
		this(text.toString(), config);
	}
	
	public MRVertex(String s, Configuration config) {
		this.config = config;
		
		edges = new EdgeLink[2];
		edges[INDEX_EDGES_TO] = null;
		edges[INDEX_EDGES_FROM] = null;

		// Five parts: ID; format; edges to other vertices; edges from other vertices;
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

		iterators = new ArrayList<WeakReference<EdgeHolder>>();
	}
	
	public Text toText(EdgeFormat edgeFormat) {
		StringBuilder s = new StringBuilder();
		
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
		
		String s1 = s.toString();
		String s2 = toTextInternal();
		if (s2 != null)
			s1 = s1 + s2;
		
		return new Text(s1);
	}
	
	//
	
	protected static byte getFlags(BytesWritable writable) {
		byte[] bytes = writable.getBytes();
		return bytes[1];
	}
	
	protected Tail getTail() {
		int tail = NO_VERTEX;
		int count = 0;
		EdgeLink edge = edges[INDEX_EDGES_TO];
		while (edge != null) {
			if (tail == NO_VERTEX)
				tail = edge.vertex;
			else if (tail != edge.vertex)
				return new Tail(NO_VERTEX, 0);
			count++;
			edge = edge.next;
		}
		return new Tail(tail, count);
	}
	
	protected class Tail {
		public final int id;
		public final int count;
		public Tail(int i, int c) { id = i; count = c; }
	}
	
	protected void compressChainInternal(MRVertex other, Configuration config) {
	}
	
	protected byte[] toWritableInternal() {
		return null;
	}
	
	
	protected void fromWritableInternal(byte[] array, int i, int n) {
	}
	
	//
	
	// For debugging only.
	
	protected String toTextInternal() {
		return null;
	}
	
	protected void fromTextInternal(String s) {
	}
	
	//
	
	private int putShort(short value, byte[] array, int i) {
		// 16 bits
		if (i + 2 > array.length)
			return i;
		
		array[i] = (byte) ((value & 0xff00) >>> 8);
		array[i+1] = (byte) (value & 0xff);
		return i + 2;
	}
	
	private short getShort(byte[] array, int i) {
		if (i + 2 > array.length)
			return Short.MIN_VALUE;
		
		short result = 0;
		result |= (array[i] << 8);
		result |= array[i+1];
		return result;
	}
	
	private int putInt(int value, byte[] array, int i) {
		// 32 bits
		if (i + 4 > array.length)
			return i;
		
		array[i] = (byte) ((value & 0xff000000) >>> 24);
		array[i+1] = (byte) ((value & 0xff0000) >>> 16);
		array[i+2] = (byte) ((value & 0xff00) >>> 8);
		array[i+3] = (byte) (value & 0xff);
		return i + 4;
	}
	
	private int getInt(byte[] array, int i) {
		if (i + 4 > array.length)
			return Integer.MIN_VALUE;
		
		int result = 0;
		result |= ((0xff & ((int) array[i])) << 24);
		result |= ((0xff & ((int) array[i+1])) << 16);
		result |= ((0xff & ((int) array[i+2])) << 8);
		result |= (0xff & ((int) array[i+3]));
		return result;
	}
	
	private void addEdge(int vertex, int which) throws IllegalArgumentException {
		if (vertex < 0)
			return;
		
		boolean allowEdgeMultiples = config.getBoolean(CONFIG_ALLOW_EDGE_MULTIPLES, false);
		
		// Keep edges sorted by getTo() to improve average-case performance
		// and to support AdjacencyMultipleIterator.

		EdgeLink link = edges[which];
		
		if (edges.length == Short.MAX_VALUE)
			throw new IllegalArgumentException("MRVertex " + getId() + " has too many edges");
		
		EdgeLink prev = null;
		while (link != null) {
			if (vertex < link.vertex) {
				break;
			}
			else if (vertex == link.vertex) {
				if (allowEdgeMultiples)
					break;
				else
					return;
			}
			prev = link;
			link = link.next;
		}
		if (prev == null)
			edges[which] = new EdgeLink(vertex, edges[which]);
		else
			prev.next = new EdgeLink(vertex, link);
	}
	
	private void removeEdge(int vertex, int which) {
		EdgeLink link = edges[which];
		EdgeLink prev = null;
		while (link != null) {
			if (vertex < link.vertex) {
				break;
			}
			else if (vertex == link.vertex) {
				// Update iterators that might be referring to the EdgeLink
				// that is about to be removed.
				
				cleanupIterators();
				for (WeakReference<EdgeHolder> ref : iterators)
					ref.get().update(link);
				
				if (prev != null)
					prev.next = link.next;
				else
					edges[which] = link.next;
				
				break;
			}
			prev = link;
			link = link.next;
		}
	}
	
	// Since there is no automatic removal of weak references to iterators
	// that have become null, this routine forces explicit removal.
	
	private void cleanupIterators() {
		Iterator<WeakReference<EdgeHolder>> it = iterators.iterator();
		while (it.hasNext()) {
			WeakReference<EdgeHolder> ref = it.next();
			if (ref.get() == null)
				it.remove();
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
	
	// A base class for classes that hold references to edges, like iterators.
	// Since the referred edge may be removed, this class can be updated to
	// advance past the removed edge.  This base class makes it simpler to
	// maintain a list of weak references to every instance that could need
	// to be updated.
	
	private class EdgeHolder {
		void update(EdgeLink link) {
			if ((link != null) && (link == current))
				current = current.next;
		}
		
		EdgeLink current;
	}
	
	private static final byte WRITABLE_TYPE_ID = 1;
	
	private static final byte FLAG_IS_BRANCH = 0x1;
	private static final byte FLAG_IS_SOURCE = 0x2;
	private static final byte FLAG_IS_SINK = 0x4;
	
	private static final String FORMAT_EDGES_TO = "t";
	private static final String FORMAT_EDGES_TO_FROM = "b";

	private static final String SEPARATOR = ";";
	private static final String EDGE_SEPARATOR = ",";
	
	private static final int INDEX_EDGES_TO = 0;
	private static final int INDEX_EDGES_FROM = 1;

	private int id;
	private Configuration config;
	private byte flags;
	private EdgeLink[] edges;
	private ArrayList<WeakReference<EdgeHolder>> iterators;
}
