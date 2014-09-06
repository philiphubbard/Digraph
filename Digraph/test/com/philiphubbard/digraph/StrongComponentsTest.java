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

public class StrongComponentsTest {
	
	public static void test() {
		System.out.println("Testing StrongComponents:");

		testSimple();
		testEdgeMultiples();
		
		System.out.println("StrongComponents passed.");
	}
	
	private static void testSimple() {
		System.out.println("Testing StrongComponents in a simple graph:");

		BasicDigraph graph = new BasicDigraph(13, Digraph.EdgeMultiples.DISABLED);
		addEdges(graph);
		
		StrongComponents<BasicDigraph.Edge> sc = new StrongComponents<BasicDigraph.Edge>(graph);
		
		int[] strongComponent = goal();		
		for (int i = 0; i < 13; i++)
			for (int j = 0; j < 13; j++)
				assert (sc.isStronglyReachable(i, j) == (strongComponent[i] == strongComponent[j]));
		
		System.out.println("StrongComponents in a simple graph passed.");
	}

	private static void testEdgeMultiples() {
		System.out.println("Testing StrongComponents with edge multiples:");

		BasicDigraph graph = new BasicDigraph(13, Digraph.EdgeMultiples.ENABLED);
		
		// Add each edge three times.
		
		for (int i = 0; i < 3; i++)
			addEdges(graph);
		
		StrongComponents<BasicDigraph.Edge> sc = new StrongComponents<BasicDigraph.Edge>(graph);
		
		int[] strongComponent = goal();		
		for (int i = 0; i < 13; i++)
			for (int j = 0; j < 13; j++)
				assert (sc.isStronglyReachable(i, j) == (strongComponent[i] == strongComponent[j]));

		System.out.println("StrongComponents with edge multiples passed.");
	}
	
	private static void addEdges(BasicDigraph graph) {
		graph.addEdge(0, new BasicDigraph.Edge(1));
		graph.addEdge(0, new BasicDigraph.Edge(5));
		graph.addEdge(0, new BasicDigraph.Edge(6));
		graph.addEdge(2, new BasicDigraph.Edge(0));
		graph.addEdge(2, new BasicDigraph.Edge(3));
		graph.addEdge(3, new BasicDigraph.Edge(2));
		graph.addEdge(3, new BasicDigraph.Edge(5));
		graph.addEdge(4, new BasicDigraph.Edge(2));
		graph.addEdge(4, new BasicDigraph.Edge(3));
		graph.addEdge(4, new BasicDigraph.Edge(11));
		graph.addEdge(5, new BasicDigraph.Edge(4));
		graph.addEdge(6, new BasicDigraph.Edge(4));
		graph.addEdge(6, new BasicDigraph.Edge(9));
		graph.addEdge(7, new BasicDigraph.Edge(6));
		graph.addEdge(7, new BasicDigraph.Edge(8));
		graph.addEdge(8, new BasicDigraph.Edge(7));
		graph.addEdge(8, new BasicDigraph.Edge(9));
		graph.addEdge(9, new BasicDigraph.Edge(10));
		graph.addEdge(9, new BasicDigraph.Edge(11));
		graph.addEdge(10, new BasicDigraph.Edge(12));
		graph.addEdge(11, new BasicDigraph.Edge(12));
		graph.addEdge(12, new BasicDigraph.Edge(9));		
	}

	private static int[] goal() {
		int[] strongComponent = new int[13];
		strongComponent[0] = 2;
		strongComponent[1] = 1;
		strongComponent[2] = 2;
		strongComponent[3] = 2;
		strongComponent[4] = 2;
		strongComponent[5] = 2;
		strongComponent[6] = 2;
		strongComponent[7] = 3;
		strongComponent[8] = 3;
		strongComponent[9] = 0;
		strongComponent[10] = 0;
		strongComponent[11] = 0;
		strongComponent[12] = 0;
		return strongComponent;
	}
}
