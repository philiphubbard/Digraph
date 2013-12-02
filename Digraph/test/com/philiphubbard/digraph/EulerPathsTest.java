// Copyright (c) 2013 Philip M. Hubbard
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
import java.util.ArrayDeque;

// Confidence tests for the EulerPaths class.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

public class EulerPathsTest {

	public static void test() {
		System.out.println("Testing EulerPaths:");

		BasicDigraph graph = new BasicDigraph(10);
		
		graph.addEdge(0, new BasicDigraph.Edge(1));
		graph.addEdge(0, new BasicDigraph.Edge(5));
		graph.addEdge(1, new BasicDigraph.Edge(2));
		graph.addEdge(2, new BasicDigraph.Edge(0));
		graph.addEdge(2, new BasicDigraph.Edge(4));
		graph.addEdge(3, new BasicDigraph.Edge(2));
		graph.addEdge(4, new BasicDigraph.Edge(3));
		graph.addEdge(4, new BasicDigraph.Edge(6));
		graph.addEdge(5, new BasicDigraph.Edge(4));
		graph.addEdge(6, new BasicDigraph.Edge(0));
		
		graph.addEdge(7, new BasicDigraph.Edge(8));
		graph.addEdge(8, new BasicDigraph.Edge(9));
		graph.addEdge(9, new BasicDigraph.Edge(7));
		
		EulerPaths<BasicDigraph.Edge> euler = new EulerPaths<BasicDigraph.Edge>(graph);
		ArrayList<ArrayDeque<Integer>> paths = euler.paths();
		
		assert (paths.size() == 2);
		
		ArrayDeque<Integer> path0 = paths.get(0);

		for (int v : path0) 
			System.out.print(v + " ");
		System.out.print("\n");

		assert (path0.size() == 11);
		int[] path0VertexCounts = new int[10];
		for (int v = 0; v < 10; v++)
			path0VertexCounts[v] = 0;
		for (int v : path0)
			path0VertexCounts[v]++;
		assert (path0VertexCounts[0] == 3);
		assert (path0VertexCounts[1] == 1);
		assert (path0VertexCounts[2] == 2);
		assert (path0VertexCounts[3] == 1);
		assert (path0VertexCounts[4] == 2);
		assert (path0VertexCounts[5] == 1);
		assert (path0VertexCounts[6] == 1);
		assert (path0VertexCounts[7] == 0);
		assert (path0VertexCounts[8] == 0);
		assert (path0VertexCounts[9] == 0);
		
		ArrayDeque<Integer> path1 = paths.get(1);
		
		for (int v : path1) 
			System.out.print(v + " ");
		System.out.print("\n");

		assert (path1.size() == 4);
		int[] path1VertexCounts = new int[10];
		for (int v = 0; v < 10; v++)
			path1VertexCounts[v] = 0;
		for (int v : path1)
			path1VertexCounts[v]++;
		assert (path1VertexCounts[0] == 0);
		assert (path1VertexCounts[1] == 0);
		assert (path1VertexCounts[2] == 0);
		assert (path1VertexCounts[3] == 0);
		assert (path1VertexCounts[4] == 0);
		assert (path1VertexCounts[5] == 0);
		assert (path1VertexCounts[6] == 0);
		assert (path1VertexCounts[7] == 2);
		assert (path1VertexCounts[8] == 1);
		assert (path1VertexCounts[9] == 1);

		System.out.println("EulerPaths passed.");
	}
	
}
