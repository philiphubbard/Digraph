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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.philiphubbard.digraph.MRBuildVertices;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class MRBuildVerticesTest {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: mrcollectvertexedgestest <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("mrbuildverticestest");
		
		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);
		MRBuildVertices.setupJob(job, inputPath, outputPath);	
		MRBuildVertices.setupPartitioning(job, "branch", "chain");
		
		if (!job.waitForCompletion(true))
			System.exit(1);
		
		//
		
		FileSystem fileSystem = FileSystem.get(conf);
		
		FileStatus[] files = fileSystem.listStatus(outputPath);
		for (FileStatus status : files) {
			Path path = status.getPath();
			boolean isBranch = path.getName().startsWith("branch");
			boolean isChain = path.getName().startsWith("chain");
			if (isBranch || isChain) {
				System.out.println(path); 
				
				FSDataInputStream in = fileSystem.open(path);
				InputStreamReader inReader = new InputStreamReader(in);
				BufferedReader bufferedReader = new BufferedReader(inReader);
				String line;
				while((line= bufferedReader.readLine()) != null) 
					System.out.println(line);
				in.close();
			}
		}
		
		fileSystem.close();
		
		//
		
		System.exit(0);
	}

}
