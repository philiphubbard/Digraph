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

// Numerical solutions to some performance analysis questions for MRCompressChains.
// Uses assert(), so must be run with a run configuration that includes "-ea" in the 
// VM arguments.

public class MRCompressChainsAnalyze {

	public static void analyze() {
		System.out.println("Analyzing MRCompressChains:");
		
		final int MAX_LENGTH = 61;
		
		long[][] compressionCounts = computeCompressionCounts(MAX_LENGTH);
		
		float[] expectedIterationsStopAt1 = 
				computeExpectedIterations(MAX_LENGTH, compressionCounts, 1);
		
		float[] expectedFinalLengthStopAt1 =
				computeExpectedFinalLength(MAX_LENGTH, compressionCounts, 1);

		float[] expectedIterationsStopAt2 = 
				computeExpectedIterations(MAX_LENGTH, compressionCounts, 2);
		
		float[] expectedFinalLengthStopAt2 =
				computeExpectedFinalLength(MAX_LENGTH, compressionCounts, 2);

		System.out.println("Expected values:\n(\"@i\" means stop after i iterations without compression)");
		System.out.format("length\tfinal length @1\titerations @1\tfinal length @2\titerations @2\n");
		
		for (int length = 2; length < MAX_LENGTH; length++) {
			if ((length < 10) && (length % 2 != 0))
				continue;
			if ((length >= 10) && (length % 10 != 0))
				continue;
			System.out.format("%d,\t%f,\t%f,\t%f,\t%f\n", length,
					expectedFinalLengthStopAt1[length],
					expectedIterationsStopAt1[length],
					expectedFinalLengthStopAt2[length],
					expectedIterationsStopAt2[length]);
		}
		
		System.out.println("MRCompressChains analysis done.");
	}
	
	// Returns c[n][m], how many n-digit binary numbers have m 01s, which is also
	// the number of ways there could be m compressions in a chain n long,
	// for 2 <= n < maxLength.
	
	private static long[][] computeCompressionCounts(int maxLength) {
		long[][] compressionCounts = new long[maxLength][maxLength];
		
		// The computation uses dynamic programming as follows.  
		// A n-digit binary number could start with 1.  In that case,
		// c[n][i] = c[n - 1][i] for all i up to n, 
		// because the initial 1 adds no occurrences of 01 beyond what was present 
		// in the numbers without the initial 1 (n-1-digit numbers).
		// If the n-digit binary number does not start with 1, it could start
		// with 01.  In that case,
		// c[n][i] += c[n - 2][i - 1] + 1 for all i up to n,
		// because the initial 01 adds 1 occurrence of 01 beyond what was present
		// in the numbers without the initial 01 (n-2-digit numbers).
		// If the n-digit binary number does not start with 01, it could start
		// with 001.  In that case,
		// c[n][i] += c[n - 3][i - 1] + 1 for all i up to n,
		// by a similar argument.
		// Likewise, the number could start with 0001, giving
		// c[n][i] += c[n - 4][i - 1] + 1, etc.
		// These relationships lead to the inner loop, below.
		
		compressionCounts[0][0] = 1;
		compressionCounts[1][0] = 2;
		
		long totalForLength = 4;
		for (int length = 2; length < maxLength; length++) {
			long counted = 0;
			int offset = 0;
			for (int otherLength = length - 1; otherLength >= 0; otherLength--) {
				for (int count = 1; count < length; count++) {
					long other = compressionCounts[otherLength][count - offset];
					compressionCounts[length][count] += other;
					counted += other;
				}
				offset = 1;
			}
			
			compressionCounts[length][0] = totalForLength - counted;

			// Update totalForLength to be 2^n for the next iteration's n.
			
			totalForLength *= 2;
		}
	
		return compressionCounts;
	}

	// Returns e[n], the expected value for the number of iterations involved in compressing a
	// chain of length n, with the termination policy for the iteration being to stop if
	// stopAt consecutive iterations have no compressions.  The probabilities involved are
	// computed from the compressionCounts array.
	
	private static float[] computeExpectedIterations(int maxLength, long[][] compressionCounts, 
			int stopAt) {
		
		// For this analysis, we care only about stopping at one or two iterations with
		// no compressions.
		
		assert ((stopAt == 1) || (stopAt == 2));
		
		// The computation uses dynamic programming as described in the loops, below.
		
		float[] expectedIterations = new float[maxLength];
		
		long totalForLength = 4;
		for (int length = 2; length < maxLength; length++) {
			float expected = 0;
			for (int numCompressions = 1; numCompressions < length; numCompressions++) {
				
				// For a particular number of compressions, the expected value increases
				// by one plus the expected value for the chain made that much shorter,
				// all with the probability of that number of compressions.
				
				float prob = compressionCounts[length][numCompressions] / (float) totalForLength;
				expected += prob * (1 + expectedIterations[length - numCompressions]);
			}
			expectedIterations[length] = expected;

			// All that is left is to consider the case of no compressions.
			
			float prob0Compressions = compressionCounts[length][0] / (float) totalForLength;
			if (stopAt == 1) {
				
				// If the termination condition is to stop at one iteration with no compressions,
				// then the expected value increases by just the probability of no compressions
				// (multiplied by one).

				expectedIterations[length] += prob0Compressions;
			}
			else if (stopAt == 2) {
				
				// If the termination condition is to stop at two iterations with no compressions,
				// the expected value increases by one plus the expected value for the current
				// length again, with the only change being that now another case of no 
				// compressions terminates the iteration with just one more.  Fortunately, the
				// expected value for the current length again is what we computed earlier and
				// stored in "expected".

				expectedIterations[length] += 
						prob0Compressions * (1 + expected + prob0Compressions * 1);
			}

			// Update totalForLength to be 2^n for the next iteration's n.
			
			totalForLength *= 2;
		}
		
		return expectedIterations;
	}
	
	// Returns e'[n], the expected value for the final length of a chain that starts out having
	// length n and gets compressed, with the termination policy for the iteration being to stop
	// if stopAt consecutive iterations have no compressions.  The probabilities involved are
	// computed from the compressionCounts array.
	
	private static float[] computeExpectedFinalLength(int maxLength, long[][] compressionCounts, 
			int stopAt) {
		
		// For this analysis, we care only about stopping at one or two iterations with
		// no compressions.
		
		assert ((stopAt == 1) || (stopAt == 2));
		
		// The computation uses dynamic programming in a manner very similar to how we used it
		// to compute the expected value for the number of iterations.

		float[] expectedFinalLength = new float[maxLength];
		expectedFinalLength[1] = 1;
		
		long totalForLength = 4;
		for (int length = 2; length < maxLength; length++) {
			float expected = 0;
			for (int numCompressions = 1; numCompressions < length; numCompressions++) {
				
				// For a particular number of compressions, the expected value is the
				// expected value for the chain made that much shorter, with the probability 
				// of that number of compressions.
				
				float prob = compressionCounts[length][numCompressions] / (float) totalForLength;
				expected += prob * expectedFinalLength[length - numCompressions];
			}
			expectedFinalLength[length] = expected;

			// All that is left is to consider the case of no compressions.
			
			float prob0Compressions = compressionCounts[length][0] / (float) totalForLength;
			if (stopAt == 1) {
				
				// If the termination condition is to stop at one iteration with no compressions,
				// the final length in this case will be the length being considered.

				expectedFinalLength[length] += prob0Compressions * length;
			}
			else if (stopAt == 2) {
				
				// If the termination condition is to stop at two iterations with no compressions,
				// the expected value increases by the expected value for the current length again,
				// with the only change being that now another case of no compressions makes the
				// final length be the length being considered.

				expectedFinalLength[length] += 
						prob0Compressions * (expected + prob0Compressions * length);
			}
			
			// Update totalForLength to be 2^n for the next iteration's n.
			
			totalForLength *= 2;
		}
		
		return expectedFinalLength;
	}

}
