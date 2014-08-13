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

// A sample driver application for running the MRBuildVertices class
// with Hadoop.

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.philiphubbard.digraph.MRBuildVertices;

public class MRBuildVerticesTest {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		setupTest(conf);
		
		Job job = Job.getInstance(conf);
		job.setJobName("mrbuildverticestest");
		
		MRBuildVertices.setupJob(job, new Path(testInput), new Path(testOutput));
		conf.setBoolean(MRBuildVertices.CONFIG_PARTITION_BRANCHES_CHAINS, true);
		
		if (!job.waitForCompletion(true))
			System.exit(1);
		
		cleanupTest(conf);

		System.exit(0);
	}
	
	private static void setupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path path = new Path(testInput);
		if (fileSystem.exists(path))
			fileSystem.delete(path, true);
		
		ArrayList<MRVertex> vertices = new ArrayList<MRVertex>();
		
		MRVertex v0 = new MRVertex(0, conf);
		v0.addEdgeTo(2);
		vertices.add(v0);
		
		MRVertex v1 = new MRVertex(1, conf);
		v1.addEdgeTo(2);
		vertices.add(v1);
		
		MRVertex v2 = new MRVertex(2, conf);
		v2.addEdgeTo(3);
		vertices.add(v2);
		
		MRVertex v3 = new MRVertex(3, conf);
		v3.addEdgeTo(4);
		vertices.add(v3);
		
		MRVertex v4 = new MRVertex(4, conf);
		v4.addEdgeTo(5);
		v4.addEdgeTo(6);
		vertices.add(v4);
		
		MRVertex v5 = new MRVertex(5, conf);
		vertices.add(v5);
		
		MRVertex v6 = new MRVertex(6, conf);
		v6.addEdgeTo(7);
		vertices.add(v6);
		
		MRVertex v7 = new MRVertex(7, conf);
		vertices.add(v7);
		
		FSDataOutputStream out = fileSystem.create(path);
		for (MRVertex vertex : vertices) {
			Text text = vertex.toText(MRVertex.EdgeFormat.EDGES_TO);
			byte[] bytes = text.copyBytes();
			for (byte b : bytes)
				out.write(b);
			out.write('\n');
		}
		out.close();
		
		fileSystem.close();
	}
	
	private static void cleanupTest(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		ArrayList<MRVertex> branch = new ArrayList<MRVertex>();
		
		FileStatus[] branchFiles = fileSystem.listStatus(new Path(testOutput + "/branch"));
		for (FileStatus status : branchFiles)
			readVertices(status, branch, conf);
		
		for (MRVertex vertex : branch) 
			System.out.println(vertex.toDisplayString());
		
		ArrayList<MRVertex> chain = new ArrayList<MRVertex>();
		
		FileStatus[] chainFiles = fileSystem.listStatus(new Path(testOutput + "/chain"));
		for (FileStatus status : chainFiles) 
			readVertices(status, chain, conf);
		
		for (MRVertex vertex : chain) 
			System.out.println(vertex.toDisplayString());
		
		fileSystem.delete(new Path(testInput), true);
		fileSystem.delete(new Path(testOutput), true);
		
		fileSystem.close();
	}
	
	private static void readVertices(FileStatus status, ArrayList<MRVertex> vertices, Configuration conf)
			throws IOException {
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
	
	private static String testInput = new String("MRBuildVerticesTest_in.txt");
	private static String testOutput = new String("MRBuildVerticesTest_out");

}
