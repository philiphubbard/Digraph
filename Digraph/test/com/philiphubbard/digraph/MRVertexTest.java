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
		testBasic();
		testSubclass();
	}
	
	private static void testBasic() {
		System.out.println("Testing MRVertex (basic):");

		MRVertex.setAllowEdgeMultiples(false);
		
		MRVertex v0 = new MRVertex(0);
		assert (v0.getId() == 0);
		
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
		assert (v1.getId() == 1);
		
		v1.addEdgeTo(0);
		v1.addEdgeTo(2);
		// The second edge 1->2 will be ignored.
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
		
		MRVertex.setAllowEdgeMultiples(true);		
		
		MRVertex v2 = new MRVertex(2);
		assert (v2.getId() == 2);
		
		v2.addEdgeTo(0);
		v2.addEdgeTo(1);
		// The second edge 2->1 will be preserved.
		v2.addEdgeTo(1);
		v2.addEdgeFrom(4);
		v2.addEdgeFrom(5);
		v2.addEdgeFrom(5);
		v2.addEdgeFrom(6);

		MRVertex.AdjacencyIterator toIt2 = v2.createToAdjacencyIterator();
		int to2 = 0;
		for (int to = toIt2.begin(); !toIt2.done(); to = toIt2.next()) {
			assert ((to == 0) || (to == 1));
			to2++;
		}
		assert (to2 == 3);

		MRVertex.AdjacencyIterator fromIt2 = v2.createFromAdjacencyIterator();
		int from2 = 0;
		for (int from = fromIt2.begin(); !fromIt2.done(); from = fromIt2.next()) {
			assert ((from == 4) || (from == 5) || (from == 6));
			from2++;
		}
		assert (from2 == 4);
		
		//
		
		Text textNotVertex = new Text("Not a vertex");
		assert (!MRVertex.getIsMRVertex(textNotVertex));
		
		Text text0a = v0.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsMRVertex(text0a));
		assert (MRVertex.getIsBranch(text0a) == false);
		MRVertex v0a = new MRVertex(text0a);

		assert (v0a.getId() == 0);
		
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
		
		Text text0b = v0.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsMRVertex(text0b));
		assert (MRVertex.getIsBranch(text0b) == false);
		MRVertex v0b = new MRVertex(text0b);

		assert (v0b.getId() == 0);
		assert (v0.equals(v0b));
		
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

		Text text1a = v1.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsMRVertex(text1a));
		assert (MRVertex.getIsBranch(text1a) == true);
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

		Text text1b = v1.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsMRVertex(text1b));
		assert (MRVertex.getIsBranch(text1b) == true);
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
		
		assert (v1.equals(v1b));
		assert (!v1.equals(v0));
		
		//
		
		MRVertex v3 = new MRVertex(3);
		v3.addEdgeTo(1);
		
		Text text3a = v3.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text3a) == false);
		
		Text text3b = v3.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text3b) == false);
		
		MRVertex v4 = new MRVertex(4);
		v4.addEdgeTo(2);
		v4.addEdgeFrom(7);
		
		Text text4a = v4.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text4a) == false);
		
		Text text4b = v4.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text4b) == false);
		
		MRVertex v5 = new MRVertex(5);
		v5.addEdgeTo(2);
		v5.addEdgeTo(2);
		
		Text text5a = v5.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text5a) == false);
		
		Text text5b = v5.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text5b) == false);
		
		MRVertex v6 = new MRVertex(6);
		v6.addEdgeTo(2);
		v6.addEdgeFrom(7);
		v6.addEdgeFrom(7);
		
		Text text6a = v6.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text6a) == false);
		
		Text text6b = v6.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text6b) == false);
		
		MRVertex v8 = new MRVertex(8);		
		v8.addEdgeFrom(9);

		Text text8a = v8.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text8a) == false);
		
		Text text8b = v8.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text8b) == false);
		
		MRVertex v9 = new MRVertex(9);
		v9.addEdgeTo(10);
		v9.addEdgeTo(10);
		v9.addEdgeTo(11);
		
		Text text9a = v9.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text9a) == true);

		Text text9b = v9.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text9b) == true);

		MRVertex v10 = new MRVertex(10);
		v10.addEdgeFrom(9);
		v10.addEdgeFrom(9);
		v10.addEdgeTo(11);
		v10.addEdgeTo(12);
		
		Text text10a = v10.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text10a) == true);

		Text text10b = v10.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text10b) == true);

		MRVertex v11 = new MRVertex(11);
		v11.addEdgeFrom(10);
		v11.addEdgeFrom(12);
		v11.addEdgeTo(13);
		v11.addEdgeTo(13);
		
		Text text11a = v11.toText(MRVertex.EdgeFormat.EDGES_TO);
		assert (MRVertex.getIsBranch(text11a) == true);
		
		Text text11b = v11.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		assert (MRVertex.getIsBranch(text11b) == true);
		
		//
		
		MRVertex v20 = new MRVertex(20);
		v20.addEdgeTo(21);
		
		MRVertex.AdjacencyIterator toIt20 = v20.createToAdjacencyIterator();
		int to20 = 0;
		for (int to = toIt20.begin(); !toIt20.done(); to = toIt20.next()) {
			assert (to == 21);
			to20++;
		}
		assert (to20 == 1);		
		
		MRVertex v21 = new MRVertex(21);
		v21.addEdgeTo(22);
		
		MRVertex.AdjacencyIterator toIt21 = v21.createToAdjacencyIterator();
		int to21 = 0;
		for (int to = toIt21.begin(); !toIt21.done(); to = toIt21.next()) {
			assert (to == 22);
			to21++;
		}
		assert (to21 == 1);		
		
		MRVertex v22 = new MRVertex(22);
		v22.addEdgeTo(23);
		
		MRVertex.AdjacencyIterator toIt22 = v22.createToAdjacencyIterator();
		int to22 = 0;
		for (int to = toIt22.begin(); !toIt22.done(); to = toIt22.next()) {
			assert (to == 23);
			to22++;
		}
		assert (to22 == 1);		
		
		MRVertex v23 = new MRVertex(23);
		v23.addEdgeTo(24);
		
		MRVertex.AdjacencyIterator toIt23 = v23.createToAdjacencyIterator();
		int to23 = 0;
		for (int to = toIt23.begin(); !toIt23.done(); to = toIt23.next()) {
			assert (to == 24);
			to23++;
		}
		assert (to23 == 1);		
		
		MRVertex v24 = new MRVertex(24);
		MRVertex.AdjacencyIterator toIt24 = v24.createToAdjacencyIterator();
		assert (toIt24.done());
		
		MRVertex.merge(v20, v21, 21);

		MRVertex.AdjacencyIterator toIt20a = v20.createToAdjacencyIterator();
		int to20a = 0;
		for (int to = toIt20a.begin(); !toIt20a.done(); to = toIt20a.next()) {
			assert (to == 22);
			to20a++;
		}
		assert (to20a == 1);		

		MRVertex.merge(v22, v23, 23);

		MRVertex.AdjacencyIterator toIt22a = v22.createToAdjacencyIterator();
		int to22a = 0;
		for (int to = toIt22a.begin(); !toIt22a.done(); to = toIt22a.next()) {
			assert (to == 24);
			to22a++;
		}
		assert (to22a == 1);		

		MRVertex.merge(v20, v22, 22);

		MRVertex.AdjacencyIterator toIt20b = v20.createToAdjacencyIterator();
		int to20b = 0;
		for (int to = toIt20b.begin(); !toIt20b.done(); to = toIt20b.next()) {
			assert (to == 24);
			to20b++;
		}
		assert (to20b == 1);		

		System.out.println("MRVertex (basic) passed.");
	}
	
	//

	private static class MRVertexSubclass extends MRVertex {
		public MRVertexSubclass(int id) {
			super(id);
			extra = 2 * id + 1;
		}
		
		public MRVertexSubclass(Text text) {
			super(text);
		}
		
		public int getExtra() {
			return extra;
		}
		
		protected void mergeInternal(MRVertex other) {
			if (other instanceof MRVertexSubclass) {
				MRVertexSubclass otherSubclass = (MRVertexSubclass) other;
				extra += otherSubclass.extra;
			}
		}
		
		protected String toTextInternal() {
			return String.valueOf(extra);
		}
		
		protected void fromTextInternal(String s) {
			extra = Integer.parseInt(s);
		}
		
		private int extra;
	}
	
	//
	
	private static void testSubclass() {
		System.out.println("Testing MRVertex (subclassing):");
		
		MRVertexSubclass v0 = new MRVertexSubclass(0);
		assert (v0.getExtra() == 1);
		
		Text t0a = v0.toText(MRVertex.EdgeFormat.EDGES_TO);
		MRVertexSubclass v0a = new MRVertexSubclass(t0a);
		assert (v0a.getExtra() == 1);
		
		Text t0b = v0.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		MRVertexSubclass v0b = new MRVertexSubclass(t0b);
		assert (v0b.getExtra() == 1);

		MRVertexSubclass v1 = new MRVertexSubclass(1);
		assert (v1.getExtra() == 3);
		
		v1.addEdgeTo(11);
		
		Text t1a = v1.toText(MRVertex.EdgeFormat.EDGES_TO);
		MRVertexSubclass v1a = new MRVertexSubclass(t1a);
		assert (v1a.getExtra() == 3);
		
		Text t1b = v1.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		MRVertexSubclass v1b = new MRVertexSubclass(t1b);
		assert (v1b.getExtra() == 3);

		MRVertexSubclass v2 = new MRVertexSubclass(2);
		assert (v2.getExtra() == 5);
		
		v2.addEdgeTo(11);
		v2.addEdgeFrom(12);
		
		Text t2a = v2.toText(MRVertex.EdgeFormat.EDGES_TO);
		MRVertexSubclass v2a = new MRVertexSubclass(t2a);
		assert (v2a.getExtra() == 5);
		
		Text t2b = v2.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		MRVertexSubclass v2b = new MRVertexSubclass(t2b);
		assert (v2b.getExtra() == 5);

		MRVertexSubclass v3 = new MRVertexSubclass(3);
		assert (v3.getExtra() == 7);
		
		v3.addEdgeFrom(13);
		
		Text t3a = v3.toText(MRVertex.EdgeFormat.EDGES_TO);
		MRVertexSubclass v3a = new MRVertexSubclass(t3a);
		assert (v3a.getExtra() == 7);
		
		Text t3b = v3.toText(MRVertex.EdgeFormat.EDGES_TO_FROM);
		MRVertexSubclass v3b = new MRVertexSubclass(t3b);
		assert (v3b.getExtra() == 7);
		
		//
		
		MRVertexSubclass v4 = new MRVertexSubclass(4);
		v4.addEdgeTo(5);
		assert (v4.getExtra() == 9);
		
		MRVertexSubclass v5 = new MRVertexSubclass(5);
		v5.addEdgeTo(6);
		assert (v5.getExtra() == 11);
		
		v4.merge(v5);
		assert (v4.getExtra() == 20);

		System.out.println("MRVertex (subclassing) passed.");
	}
	
}
