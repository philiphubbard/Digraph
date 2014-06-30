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

// Confidence tests for the MRVertex class.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

package com.philiphubbard.digraph;

import com.philiphubbard.digraph.MRVertex;
import org.apache.hadoop.io.Text;

public class MRVertexTest {

	public static void test() {
		System.out.println("Testing MRVertex:");

		//
		
		MRVertex v0 = new MRVertex(0);
		assert (v0.id() == 0);
		
		MRVertex.AdjacencyIterator toIt0 = v0.createToAdjacencyIterator();
		int to0 = 0;
		for (toIt0.begin(); !toIt0.done(); toIt0.next())
			to0++;
		assert (to0 == 0);

		MRVertex.AdjacencyIterator fromIt0 = v0.createFromAdjacencyIterator();
		int from0 = 0;
		for (fromIt0.begin(); !fromIt0.done(); fromIt0.next())
			from0++;
		assert (from0 == 0);
		
		//
		
		MRVertex v1 = new MRVertex(1);
		assert (v1.id() == 1);
		
		v1.addEdgeTo(0);
		v1.addEdgeTo(2);
		v1.addEdgeFrom(3);
		
		MRVertex.AdjacencyIterator toIt1 = v1.createToAdjacencyIterator();
		int to1 = 0;
		for (int to = toIt1.begin(); !toIt1.done(); to = toIt1.next()) {
			assert ((to == 0) || (to == 2));
			to1++;
		}
		assert (to1 == 2);

		MRVertex.AdjacencyIterator fromIt1 = v1.createFromAdjacencyIterator();
		int from1 = 0;
		for (int from = fromIt1.begin(); !fromIt1.done(); from = fromIt1.next()) {
			assert (from == 3);
			from1++;
		}
		assert (from1 == 1);
		
		//
		
		Text text0a = v0.toText(MRVertex.FORMAT_TO_EDGES);
		MRVertex v0a = new MRVertex(text0a);

		assert (v0a.id() == 0);
		
		MRVertex.AdjacencyIterator toIt0a = v0a.createToAdjacencyIterator();
		int to0a = 0;
		for (toIt0a.begin(); !toIt0a.done(); toIt0a.next())
			to0a++;
		assert (to0a == 0);

		MRVertex.AdjacencyIterator fromIt0a = v0a.createFromAdjacencyIterator();
		int from0a = 0;
		for (fromIt0a.begin(); !fromIt0a.done(); fromIt0a.next())
			from0a++;
		assert (from0a == 0);

		//
		
		Text text0b = v0.toText(MRVertex.FORMAT_TO_FROM_EDGES);
		MRVertex v0b = new MRVertex(text0b);

		assert (v0b.id() == 0);
		
		MRVertex.AdjacencyIterator toIt0b = v0b.createToAdjacencyIterator();
		int to0b = 0;
		for (toIt0b.begin(); !toIt0b.done(); toIt0b.next())
			to0b++;
		assert (to0b == 0);

		MRVertex.AdjacencyIterator fromIt0b = v0b.createFromAdjacencyIterator();
		int from0b = 0;
		for (fromIt0b.begin(); !fromIt0b.done(); fromIt0b.next())
			from0b++;
		assert (from0b == 0);

		//

		Text text1a = v1.toText(MRVertex.FORMAT_TO_EDGES);
		MRVertex v1a = new MRVertex(text1a);

		MRVertex.AdjacencyIterator toIt1a = v1a.createToAdjacencyIterator();
		int to1a = 0;
		for (int to = toIt1a.begin(); !toIt1a.done(); to = toIt1a.next()) {
			assert ((to == 0) || (to == 2));
			to1a++;
		}
		assert (to1a == 2);

		MRVertex.AdjacencyIterator fromIt1a = v1a.createFromAdjacencyIterator();
		int from1a = 0;
		for (fromIt1a.begin(); !fromIt1a.done(); fromIt1a.next()) {
			from1a++;
		}
		assert (from1a == 0);
		
		//

		Text text1b = v1.toText(MRVertex.FORMAT_TO_FROM_EDGES);
		MRVertex v1b = new MRVertex(text1b);

		MRVertex.AdjacencyIterator toIt1b = v1b.createToAdjacencyIterator();
		int to1b = 0;
		for (int to = toIt1b.begin(); !toIt1b.done(); to = toIt1b.next()) {
			assert ((to == 0) || (to == 2));
			to1b++;
		}
		assert (to1b == 2);

		MRVertex.AdjacencyIterator fromIt1b = v1b.createFromAdjacencyIterator();
		int from1b = 0;
		for (int from = fromIt1b.begin(); !fromIt1b.done(); from = fromIt1b.next()) {
			assert (from == 3);
			from1b++;
		}
		assert (from1b == 1);
		
		//

		System.out.println("MRVertex passed.");
	}

}
