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

import java.util.ArrayList;

// An abstract base class for a directed graph.
// This class is a generic, parameterized by the edge class.
// Algorithms that do not need to add edges can work with this class' interface.

public abstract class Digraph<E extends Digraph.Edge> {
	
	// Constructor.  It can have edges involving vertices with indices
	// in the range from 0 to vertexCapacity - 1.  The EdgeMultiples enum
	// specifies whether the graph can have more than one edge between
	// a pair of vertices or not.
	
	public enum EdgeMultiples { ENABLED, DISABLED }
	
	public Digraph(int vertexCapacity, EdgeMultiples multiples) {
		allowMultiples = (multiples == EdgeMultiples.ENABLED);
		edges = new ArrayList<EdgeLink>(vertexCapacity);
		for (int i = 0; i < vertexCapacity; i++)
			edges.add(null);
	}
	
	public static final int NO_VERTEX = -1;
	
	// A base class for the edges of the graph.
	
	public static class Edge {
		
		// Constructor, specifying the the vertex the edge is directed to.
		// The vertex it is directed from is implicit as the argument to the
		// Digraph.addEdge() routine.
		
		public Edge(int to) {
			this.to = to;
		}
		
		// The vertex the edge is pointing to.
		
		public int getTo() {
			return to;
		}
		
		private int to;
	}
	
	// Base class for an iterator over the edges adjacent to (directed from)
	// the specified vertex.  It can be used in a loop like the following:
	// "for (Edge e = iterator.begin(); !iterator.done(); e = iterator.next())"
	
	public class AdjacencyIterator  {
		
		// Returns the first edge in the iteration.
		
		public E begin() {
			current = graph.edges.get(from);
			return (current != null) ? current.edge : null;
		}
		
		// Returns the next edge in the iteration.
		
		public E next() {
			if (current != null)
				current = current.next;
			return (current != null) ? current.edge : null;
		}
		
		// Returns true if the iteration is done.
		
		public boolean done() {
			return (current == null);
		}
		
		// The derived class function that creates this iterator should
		// ensure that the vertex is in range.
		
		protected AdjacencyIterator(Digraph<E> graph, int from) {
			this.graph = graph;
			this.from = from;
			current = null;
		}
		
		private Digraph<E> graph;
		private int from;
		private EdgeLink current;
	}
	
	// A derived class must define this function to create the iterator
	// for edges from the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	abstract public AdjacencyIterator createAdjacencyIterator(int from)
			throws IndexOutOfBoundsException;
	
	// The graph can have vertices with indices in the range from 
	// 0 to vertexCapacity() - 1.
	
	public int getVertexCapacity() {
		return edges.size();
	}
	
	// Whether the graph can have more than one edge between
	// a pair of vertices or not.
	
	public EdgeMultiples getEdgeMultiples() {
		if (allowMultiples)
			return EdgeMultiples.ENABLED;
		else
			return EdgeMultiples.DISABLED;
	}
	
	// The number of edges directed out from the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	public int getOutDegree(int from) throws IndexOutOfBoundsException {
		if ((from < 0) || (getVertexCapacity() <= from))
			throw new IndexOutOfBoundsException("Digraph.outDegree() " +
											    "vertex out of range");
		cacheDegrees();
		return outDegrees.get(from);
	}
	
	// The number of edges directed in to the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	public int getInDegree(int to) throws IndexOutOfBoundsException {
		if ((to < 0) || (getVertexCapacity() <= to))
			throw new IndexOutOfBoundsException("Digraph.inDegree() " +
											    "vertex out of range");
		cacheDegrees();
		return inDegrees.get(to);
	}
	
	//
	
	protected void addEdge(int from, E newEdge) {
		if ((from < 0) || (edges.size() <= from))
			return;
		if ((newEdge.getTo() < 0) || (edges.size() <= newEdge.getTo()))
			return;
		if (allowMultiples) {
			edges.set(from, new EdgeLink(newEdge, edges.get(from)));
		}
		else {
			// Keep edges sorted by getTo() to improve average-case performance.
			EdgeLink link = edges.get(from);
			EdgeLink prev = null;
			while (link != null) {
				if (newEdge.getTo() < link.edge.getTo())
					break;
				else if (newEdge.getTo() == link.edge.getTo())
					return;
				prev = link;
				link = link.next;	
			}
			if (prev == null)
				edges.set(from, new EdgeLink(newEdge, edges.get(from)));
			else
				prev.next = new EdgeLink(newEdge, link);
		}
		if (inDegrees != null) {
			int inDegree = inDegrees.get(newEdge.getTo());
			inDegrees.set(newEdge.getTo(), (inDegree == -1) ? 1 : inDegree + 1);
		}
		if (outDegrees != null) {
			int outDegree = outDegrees.get(from);
			outDegrees.set(from, (outDegree == -1) ? 1 : outDegree + 1);
		}
	}
	
	private void cacheDegrees() {
		if ((inDegrees != null) && (outDegrees != null))
			return;
		
		inDegrees = new ArrayList<Integer>();
		outDegrees = new ArrayList<Integer>();
		for (int v = 0; v < edges.size(); v++) {
			inDegrees.add(-1);
			outDegrees.add(-1);
		}
		for (int v = 0; v < edges.size(); v++) {
			AdjacencyIterator it = createAdjacencyIterator(v);
			int outDegree = 0;
			for (E e = it.begin(); !it.done(); e = it.next()) {
				outDegree++;
				int to = e.getTo();
				int inDegree = inDegrees.get(to);
				inDegrees.set(to, (inDegree == -1) ? 1 : inDegree + 1);
				if (outDegrees.get(to) == -1)
					outDegrees.set(to, 0);
			}
			if (outDegree > 0) {
				outDegrees.set(v, outDegree);
				if (inDegrees.get(v) == -1)
					inDegrees.set(v, 0);
			}
		}
	}
	
	private class EdgeLink {
		public EdgeLink(E edge, EdgeLink next) {
			this.edge = edge;
			this.next = next;
		}
		public E edge;
		public EdgeLink next;
	}
	
	private boolean allowMultiples;
	private ArrayList<EdgeLink> edges;
	private ArrayList<Integer> inDegrees;
	private ArrayList<Integer> outDegrees;
}
