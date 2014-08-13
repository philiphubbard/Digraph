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

// A sample driver application for running the MRCompressChains class
// with Hadoop.

import com.philiphubbard.digraph.MRCompressChains;

import java.util.ArrayList;
import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class MRCompressChainsTest {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		setupTest(conf);
		String inputOrig = testInput;
		String outputOrig = testOutput;
		
		int iter = 0;
		boolean keepGoing = true;
		MRCompressChains.beginIteration();
		while (keepGoing) {
			Job job = Job.getInstance(conf);
			job.setJobName("mrcompresschainstest");
			
			MRCompressChains.setupIterationJob(job, new Path(inputOrig), new Path(outputOrig));
			
			if (!job.waitForCompletion(true))
				System.exit(1);
			
			iter++;
			keepGoing = MRCompressChains.continueIteration(job, new Path(inputOrig), new Path(outputOrig));
		}
		
		//
		
		System.out.println("Number of iterations = " + iter); 
		
		cleanupTest(conf);
		
		//
		
		System.exit(0);
	}
	
	private static void setupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path path = new Path(testInput);
		if (fileSystem.exists(path))
			fileSystem.delete(path, true);
		
		ArrayList<MRVertex> vertices = new ArrayList<MRVertex>();
		for (int i = 0; i < 60; i++) {
			MRVertex vertex = new MRVertex(i, conf);
			vertices.add(vertex);
			if (i % 20 != 19)
				vertex.addEdgeTo(i+1);
		}
		
	    SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
	    		SequenceFile.Writer.file(path), 
	    		SequenceFile.Writer.keyClass(IntWritable.class), 
	    		SequenceFile.Writer.valueClass(BytesWritable.class));
	    for (MRVertex vertex : vertices) 
	    	writer.append(new IntWritable(vertex.getId()), vertex.toWritable(MRVertex.EdgeFormat.EDGES_TO));
	    writer.close();
		
		fileSystem.close();
	}
	
	private static void cleanupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		ArrayList<MRVertex> vertices = new ArrayList<MRVertex>();
		
		FileStatus[] files = fileSystem.listStatus(new Path(testOutput));
		for (FileStatus status : files) {
			Path path = status.getPath();
			if (path.getName().startsWith("part")) {
				System.out.println(path); 
				
			    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			    IntWritable key = new IntWritable();
			    BytesWritable value = new BytesWritable();
			    while (reader.next(key, value))
			    	vertices.add(new MRVertex(value, conf));
			    reader.close();
			}
		}
		
		for (MRVertex vertex : vertices) 
			System.out.println(vertex.toDisplayString());
		
		fileSystem.delete(new Path(testInput), true);
		fileSystem.delete(new Path(testOutput), true);
		
		fileSystem.close();
	}
	
	private static String testInput = new String("MRCompressChainsTest_in.txt");
	private static String testOutput = new String("MRCompressChainsTest_out");
}
