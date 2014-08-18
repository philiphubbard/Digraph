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
		
		//
		
		BasicDigraph graph2 = new BasicDigraph(16, multiples);
		
		graph2.addEdge(0, new BasicDigraph.Edge(1));
		graph2.addEdge(0, new BasicDigraph.Edge(2));
		graph2.addEdge(0, new BasicDigraph.Edge(3));
		graph2.addEdge(0, new BasicDigraph.Edge(4));
		graph2.addEdge(0, new BasicDigraph.Edge(5));
		
		graph2.addEdge(1, new BasicDigraph.Edge(6));
		graph2.addEdge(1, new BasicDigraph.Edge(7));
		graph2.addEdge(1, new BasicDigraph.Edge(8));
		graph2.addEdge(1, new BasicDigraph.Edge(9));
		graph2.addEdge(1, new BasicDigraph.Edge(10));
		
		graph2.addEdge(2, new BasicDigraph.Edge(11));
		graph2.addEdge(2, new BasicDigraph.Edge(12));
		graph2.addEdge(2, new BasicDigraph.Edge(13));
		graph2.addEdge(2, new BasicDigraph.Edge(14));
		graph2.addEdge(2, new BasicDigraph.Edge(15));
		
		BasicDigraph.AdjacencyIterator it0a = graph2.createAdjacencyIterator(0);
		BasicDigraph.AdjacencyIterator it1a = graph2.createAdjacencyIterator(1);
		BasicDigraph.AdjacencyIterator it2a = graph2.createAdjacencyIterator(2);
		it0a.begin();
		it1a.begin();
		it2a.begin();
		{
			BasicDigraph.AdjacencyIterator it0b = graph2.createAdjacencyIterator(0);
			BasicDigraph.AdjacencyIterator it1b = graph2.createAdjacencyIterator(1);
			BasicDigraph.AdjacencyIterator it2b = graph2.createAdjacencyIterator(2);
			it0b.begin();
			it0b.next();
			it1b.begin();
			it1b.next();
			it2b.begin();
			it2b.next();
			{
				BasicDigraph.AdjacencyIterator it0c = graph2.createAdjacencyIterator(0);
				BasicDigraph.AdjacencyIterator it1c = graph2.createAdjacencyIterator(1);
				BasicDigraph.AdjacencyIterator it2c = graph2.createAdjacencyIterator(2);
				it0c.begin();
				it0c.next();
				it0c.next();
				it1c.begin();
				it1c.next();
				it1c.next();
				it2c.begin();
				it2c.next();
				it2c.next();
				
				graph2.removeEdge(0, 2);
				
				graph2.removeEdge(1, 7);
				graph2.removeEdge(1, 8);
				
				graph2.removeEdge(2, 12);
				graph2.removeEdge(2, 13);
				graph2.removeEdge(2, 14);

				int to0c = 0;
				for (; !it0c.done(); it0c.next())
					to0c++;
				assert (to0c == 3);

				int to1c = 0;
				for (; !it1c.done(); it1c.next())
					to1c++;
				assert (to1c == 2);

				int to2c = 0;
				for (; !it2c.done(); it2c.next())
					to2c++;
				assert (to2c == 1);
			}

			int to0b = 0;
			for (; !it0b.done(); it0b.next())
				to0b++;
			assert (to0b == 3);

			int to1b = 0;
			for (; !it1b.done(); it1b.next())
				to1b++;
			assert (to1b == 2);

			int to2b = 0;
			for (; !it2b.done(); it2b.next())
				to2b++;
			assert (to2b == 1);		
		}

		int to0a = 0;
		for (; !it0a.done(); it0a.next())
			to0a++;
		assert (to0a == 4);

		int to1a = 0;
		for (; !it1a.done(); it1a.next())
			to1a++;
		assert (to1a == 3);

		int to2a = 0;
		for (; !it2a.done(); it2a.next())
			to2a++;
		assert (to2a == 2);
		
		System.out.println("BasicDigraph simple behaviors passed.");
	}
	
	private static void testMultiples() {
		System.out.println("Testing BasicDigraph edge multiples");

		BasicDigraph graph1 = new BasicDigraph(10, BasicDigraph.EdgeMultiples.ENABLED);
		
		graph1.addEdge(0, new BasicDigraph.Edge(1));
		graph1.addEdge(1, new BasicDigraph.Edge(2));
		graph1.addEdge(2, new BasicDigraph.Edge(1));
		graph1.addEdge(2, new BasicDigraph.Edge(3));
		graph1.addEdge(2, new BasicDigraph.Edge(4));
		graph1.addEdge(2, new BasicDigraph.Edge(5));
		graph1.addEdge(2, new BasicDigraph.Edge(4));
		graph1.addEdge(2, new BasicDigraph.Edge(3));
		
		BasicDigraph.AdjacencyIterator it0 = graph1.createAdjacencyIterator(0);
		int numEdges0 = 0;
		for (it0.begin(); !it0.done(); it0.next())
			numEdges0++;
		assert (numEdges0 == 1);
		
		BasicDigraph.AdjacencyIterator it1 = graph1.createAdjacencyIterator(1);
		int numEdges1 = 0;
		for (it1.begin(); !it1.done(); it1.next())
			numEdges1++;
		assert (numEdges1 == 1);
		
		BasicDigraph.AdjacencyIterator it2 = graph1.createAdjacencyIterator(2);
		int numEdges2 = 0;
		for (it2.begin(); !it2.done(); it2.next())
			numEdges2++;
		assert (numEdges2 == 6);
		
		BasicDigraph.AdjacencyMultipleIterator itm2 = graph1.createAdjacencyMultipleIterator(2);
		int[] numMultipleEdges2 = new int[6];
		for (int i = 0; i < numMultipleEdges2.length; i++)
			numMultipleEdges2[i] = 0;
		for (ArrayList<BasicDigraph.Edge> edges = itm2.begin(); !itm2.done(); edges = itm2.next()) {
			int to = BasicDigraph.NO_VERTEX;
			for (BasicDigraph.Edge edge : edges) {
				if (to == BasicDigraph.NO_VERTEX)
					to = edge.getTo();
				else
					assert (to == edge.getTo());
				numMultipleEdges2[to]++;
			}
		}
		assert (numMultipleEdges2[0] == 0);
		assert (numMultipleEdges2[1] == 1);
		assert (numMultipleEdges2[2] == 0);
		assert (numMultipleEdges2[3] == 2);
		assert (numMultipleEdges2[4] == 2);
		assert (numMultipleEdges2[5] == 1);

		BasicDigraph.AdjacencyIterator it3 = graph1.createAdjacencyIterator(3);
		int numEdges3 = 0;
		for (it3.begin(); !it3.done(); it3.next())
			numEdges3++;
		assert (numEdges3 == 0);
		
		//
		
		BasicDigraph graph2 = new BasicDigraph(16, Digraph.EdgeMultiples.ENABLED);
		
		graph2.addEdge(0, new BasicDigraph.Edge(1));
		graph2.addEdge(0, new BasicDigraph.Edge(2));
		graph2.addEdge(0, new BasicDigraph.Edge(2));
		graph2.addEdge(0, new BasicDigraph.Edge(3));
		graph2.addEdge(0, new BasicDigraph.Edge(4));
		graph2.addEdge(0, new BasicDigraph.Edge(4));
		graph2.addEdge(0, new BasicDigraph.Edge(5));
		
		graph2.addEdge(1, new BasicDigraph.Edge(6));
		graph2.addEdge(1, new BasicDigraph.Edge(6));
		graph2.addEdge(1, new BasicDigraph.Edge(7));
		graph2.addEdge(1, new BasicDigraph.Edge(8));
		graph2.addEdge(1, new BasicDigraph.Edge(8));
		graph2.addEdge(1, new BasicDigraph.Edge(9));
		graph2.addEdge(1, new BasicDigraph.Edge(10));
		graph2.addEdge(1, new BasicDigraph.Edge(10));
		
		graph2.addEdge(2, new BasicDigraph.Edge(11));
		graph2.addEdge(2, new BasicDigraph.Edge(12));
		graph2.addEdge(2, new BasicDigraph.Edge(12));
		graph2.addEdge(2, new BasicDigraph.Edge(13));
		graph2.addEdge(2, new BasicDigraph.Edge(13));
		graph2.addEdge(2, new BasicDigraph.Edge(14));
		graph2.addEdge(2, new BasicDigraph.Edge(14));
		graph2.addEdge(2, new BasicDigraph.Edge(15));
		
		BasicDigraph.AdjacencyMultipleIterator it0a = graph2.createAdjacencyMultipleIterator(0);
		BasicDigraph.AdjacencyMultipleIterator it1a = graph2.createAdjacencyMultipleIterator(1);
		BasicDigraph.AdjacencyMultipleIterator it2a = graph2.createAdjacencyMultipleIterator(2);
		it0a.begin();
		it1a.begin();
		it2a.begin();
		{
			BasicDigraph.AdjacencyMultipleIterator it0b = graph2.createAdjacencyMultipleIterator(0);
			BasicDigraph.AdjacencyMultipleIterator it1b = graph2.createAdjacencyMultipleIterator(1);
			BasicDigraph.AdjacencyMultipleIterator it2b = graph2.createAdjacencyMultipleIterator(2);
			it0b.begin();
			it0b.next();
			it1b.begin();
			it1b.next();
			it2b.begin();
			it2b.next();
			{
				BasicDigraph.AdjacencyMultipleIterator it0c = graph2.createAdjacencyMultipleIterator(0);
				BasicDigraph.AdjacencyMultipleIterator it1c = graph2.createAdjacencyMultipleIterator(1);
				BasicDigraph.AdjacencyMultipleIterator it2c = graph2.createAdjacencyMultipleIterator(2);
				it0c.begin();
				it0c.next();
				it0c.next();
				it1c.begin();
				it1c.next();
				it1c.next();
				it2c.begin();
				it2c.next();
				it2c.next();
				
				graph2.removeEdge(0, 2);
				
				graph2.removeEdge(1, 7);
				graph2.removeEdge(1, 8);
				
				graph2.removeEdge(2, 12);
				graph2.removeEdge(2, 13);
				graph2.removeEdge(2, 13);
				graph2.removeEdge(2, 14);

				int to0c = 0;
				for (; !it0c.done(); it0c.next())
					to0c++;
				assert (to0c == 3);

				int to1c = 0;
				for (; !it1c.done(); it1c.next())
					to1c++;
				// Only one of the 1->8 edges were removed, so the iteration will still find
				// the other one.
				assert (to1c == 3);

				int to2c = 0;
				for (; !it2c.done(); it2c.next())
					to2c++;
				// Only one of the two 2->14 edges were removed.
				assert (to2c == 2);
			}

			int to0b = 0;
			for (; !it0b.done(); it0b.next())
				to0b++;
			// Only one of the 0->2 edges was removed.
			assert (to0b == 4);

			int to1b = 0;
			for (; !it1b.done(); it1b.next())
				to1b++;
			// Only one of the 1->8 edges were removed.
			assert (to1b == 3);

			int to2b = 0;
			for (; !it2b.done(); it2b.next())
				to2b++;
			// Only one of the 2->12 and 2->14 edges were removed.
			assert (to2b == 3);
		}

		int to0a = 0;
		for (; !it0a.done(); it0a.next())
			to0a++;
		// Only one of the 0->2 edges was removed.
		assert (to0a == 5);

		int to1a = 0;
		for (; !it1a.done(); it1a.next())
			to1a++;
		// Only one of the 1->8 edges were removed.
		assert (to1a == 4);

		int to2a = 0;
		for (; !it2a.done(); it2a.next())
			to2a++;
		// Only one of the 2->12 and 2->14 edges were removed.
		assert (to2a == 4);

		System.out.println("BasicDigraph edge multiples passed.");
	}

}
