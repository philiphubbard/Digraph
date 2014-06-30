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

// Confidence tests for the BasicDigraph class.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

public class BasicDigraphTest {

	public static void test() {
		System.out.println("Testing BasicDigraph:");
		
		testSimple(Digraph.EdgeMultiples.DISABLED);
		testSimple(Digraph.EdgeMultiples.ENABLED);
		testMultiples();

		System.out.println("BasicDigraph passed.");
	}
	
	private static void testSimple(Digraph.EdgeMultiples multiples) {
		System.out.println("Testing simple BasicDigraph behaviors (edge multiples "
							+ (multiples == Digraph.EdgeMultiples.ENABLED ? "enabled" : "disabled")
							+ "):");
		
		BasicDigraph graph0 = new BasicDigraph(0, multiples);
		assert (graph0.getVertexCapacity() == 0);
		
		// Trying to get an adjacency iterator for a vertex that is outside the
		// graph's capacity throws an exception.
		
		boolean failedAsExpected = false;
		try {
			graph0.createAdjacencyIterator(0);
		} catch (IndexOutOfBoundsException e) {
			failedAsExpected = true;
		}
		assert (failedAsExpected);
		
		// Adding an edge to a vertex that is outside the graph's capacity
		// silently does nothing.
		
		graph0.addEdge(1, new BasicDigraph.Edge(2));
		
		failedAsExpected = false;
		try {
			graph0.createAdjacencyIterator(1);
		} catch (IndexOutOfBoundsException e) {
			failedAsExpected = true;
		}
		assert (failedAsExpected);

		BasicDigraph graph1 = new BasicDigraph(10, multiples);
		assert (graph1.getVertexCapacity() == 10);
		
		graph1.addEdge(0, new BasicDigraph.Edge(2));
		graph1.addEdge(0, new BasicDigraph.Edge(3));
		graph1.addEdge(2, new BasicDigraph.Edge(3));
		
		BasicDigraph.AdjacencyIterator it0 = graph1.createAdjacencyIterator(0);
		int numEdges0 = 0;
		for (BasicDigraph.Edge edge = it0.begin(); !it0.done(); edge = it0.next()) {
			assert ((edge.getTo() == 2) || (edge.getTo() == 3));
			numEdges0++;
		}
		assert (numEdges0 == 2);

		// An adjacency iterator can be obtained for a vertex index that is within
		// the graph's capacity even if that vertex has not been added, and that
		// iterator does nothing.
		
		BasicDigraph.AdjacencyIterator it1 = graph1.createAdjacencyIterator(1);
		int numEdges1 = 0;
		for (it1.begin(); !it1.done(); it1.next())
			numEdges1++;
		assert (numEdges1 == 0);
		
		assert (graph1.getInDegree(0) == 0);
		assert (graph1.getOutDegree(0) == 2);
		assert (graph1.getInDegree(2) == 1);
		assert (graph1.getOutDegree(2) == 1);
		assert (graph1.getInDegree(3) == 2);
		assert (graph1.getOutDegree(3) == 0);

		System.out.println("BasicDigraph simple behaviors passed.");
	}
	
	private static void testMultiples() {
		System.out.println("Testing BasicDigraph edge multiples");

		BasicDigraph graph = new BasicDigraph(10, BasicDigraph.EdgeMultiples.ENABLED);
		
		graph.addEdge(0, new BasicDigraph.Edge(1));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(2, new BasicDigraph.Edge(1));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		
		BasicDigraph.AdjacencyIterator it0 = graph.createAdjacencyIterator(0);
		int numEdges0 = 0;
		for (it0.begin(); !it0.done(); it0.next())
			numEdges0++;
		assert (numEdges0 == 1);
		
		BasicDigraph.AdjacencyIterator it1 = graph.createAdjacencyIterator(1);
		int numEdges1 = 0;
		for (it1.begin(); !it1.done(); it1.next())
			numEdges1++;
		assert (numEdges1 == 1);
		
		BasicDigraph.AdjacencyIterator it2 = graph.createAdjacencyIterator(2);
		int numEdges2 = 0;
		for (it2.begin(); !it2.done(); it2.next())
			numEdges2++;
		assert (numEdges2 == 3);
		
		BasicDigraph.AdjacencyIterator it3 = graph.createAdjacencyIterator(3);
		int numEdges3 = 0;
		for (it3.begin(); !it3.done(); it3.next())
			numEdges3++;
		assert (numEdges3 == 0);
		
		System.out.println("BasicDigraph edge multiples passed.");
	}

}
