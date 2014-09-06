package com.philiphubbard.digraph;

import java.util.ArrayList;

// Confidence tests for the WeightedDigraph class.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

public class WeightedDigraphTest {
	
	public static void test() {
		System.out.println("Testing WeightedDigraph:");
		
		WeightedDigraph graph0 = new WeightedDigraph(4, Digraph.EdgeMultiples.DISABLED);
		
		graph0.addEdge(0, new WeightedDigraph.Edge(1));
		graph0.addEdge(0, new WeightedDigraph.Edge(2));
		graph0.addEdge(1, new WeightedDigraph.Edge(3));
		graph0.addEdge(2, new WeightedDigraph.Edge(3));
		graph0.addEdge(3, new WeightedDigraph.Edge(0));

		WeightedDigraph.AdjacencyIterator it0a = graph0.createAdjacencyIterator(0);
		for (WeightedDigraph.Edge edge = it0a.begin(); !it0a.done(); edge = it0a.next()) {
			assert ((edge.getTo() == 1) || (edge.getTo() == 2));
			assert (edge.getWeight() == 0.0f);
		}
		
		graph0.removeEdge(0, 1);
		
		WeightedDigraph.AdjacencyIterator it0b = graph0.createAdjacencyIterator(0);
		for (WeightedDigraph.Edge edge = it0b.begin(); !it0b.done(); edge = it0b.next()) {
			assert (edge.getTo() == 2);
			assert (edge.getWeight() == 0.0f);
		}
		
		WeightedDigraph graph1 = new WeightedDigraph(4, Digraph.EdgeMultiples.DISABLED);
		
		graph1.addEdge(0, new WeightedDigraph.Edge(1, 10.0f));
		graph1.addEdge(1, new WeightedDigraph.Edge(2, 11.0f));
		graph1.addEdge(1, new WeightedDigraph.Edge(3, 12.0f));
		graph1.addEdge(2, new WeightedDigraph.Edge(0, 13.0f));
		graph1.addEdge(3, new WeightedDigraph.Edge(1, 14.0f));

		WeightedDigraph.AdjacencyIterator it1 = graph1.createAdjacencyIterator(1);
		for (WeightedDigraph.Edge edge = it1.begin(); !it1.done(); edge = it1.next()) {
			assert (((edge.getTo() == 2) && (edge.getWeight() == 11.0f)) ||
					((edge.getTo() == 3) && (edge.getWeight() == 12.0f)));
		}
		
		WeightedDigraph graph2 = new WeightedDigraph(4, Digraph.EdgeMultiples.ENABLED);
		
		graph2.addEdge(0, new WeightedDigraph.Edge(3, 20.0f));
		graph2.addEdge(0, new WeightedDigraph.Edge(3, 21.0f));
		graph2.addEdge(1, new WeightedDigraph.Edge(3, 22.0f));
		graph2.addEdge(1, new WeightedDigraph.Edge(3, 23.0f));
		graph2.addEdge(2, new WeightedDigraph.Edge(0, 24.0f));
		graph2.addEdge(2, new WeightedDigraph.Edge(0, 25.0f));
		graph2.addEdge(2, new WeightedDigraph.Edge(1, 26.0f));
		graph2.addEdge(2, new WeightedDigraph.Edge(1, 27.0f));

		int matches = 0;
		WeightedDigraph.AdjacencyMultipleIterator it2 = graph2.createAdjacencyMultipleIterator(2);
		for (ArrayList<WeightedDigraph.Edge> edges = it2.begin(); !it2.done(); edges = it2.next()) {
			assert (edges.size() == 2);
			for (WeightedDigraph.Edge edge : edges) {
				if ((edge.getTo() == 0) && (edge.getWeight() == 24.0f))
					matches++;
				if ((edge.getTo() == 0) && (edge.getWeight() == 25.0f))
					matches++;
				if ((edge.getTo() == 1) && (edge.getWeight() == 26.0f))
					matches++;
				if ((edge.getTo() == 1) && (edge.getWeight() == 27.0f))
					matches++;
			}
		}
		assert (matches == 4);

		System.out.println("WeightedDigraph passed.");
	}
}
