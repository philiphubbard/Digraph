package com.philiphubbard.digraph;

import com.philiphubbard.digraph.MRVertex;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MRCompressChains {

	// HEY!! Use SequenceFileOutputFormat as a more efficient intermediate format between
	// multiple map-reduce phases?
	
	public static void beginIteration() {
		iter = 0;
		numIterWithoutCompressions = 0;
	}
	
	public static void setupIterationJob(Job job, Path inputPathOrig, Path outputPathOrig)
			throws IOException {
		job.setJarByClass(MRCompressChains.class);
		job.setMapperClass(MRCompressChains.Mapper.class);
		job.setCombinerClass(MRCompressChains.Reducer.class);
		job.setReducerClass(MRCompressChains.Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		
		Path inputPath;
		if (iter == 0)
			inputPath = inputPathOrig;
		else
			inputPath = new Path(outputPathOrig.toString() + (iter - 1));
		Path outputPath = new Path(outputPathOrig.toString() + iter);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// HEY!! 
		System.out.println("** iteration " + iter + " input \"" + inputPath.toString()
				+ "\" output \"" + outputPath.toString() + "\" **");
	}
	
	public static boolean continueIteration(Job job, Path inputPathOrig, Path outputPathOrig) 
			throws IOException {
		FileSystem fileSystem = FileSystem.get(job.getConfiguration());

		if (iter > 0) {
			Path outputPathOld = new Path(outputPathOrig.toString() + (iter - 1));
			
			// HEY!! 
			System.out.println("** deleting \"" + outputPathOld.toString() + "\" **");

			if (fileSystem.exists(outputPathOld))
				fileSystem.delete(outputPathOld, true);
		}

		Counters jobCounters = job.getCounters();
		long numCompressions = 
				jobCounters.findCounter(MRCompressChains.CompressionCounter.numCompressions).getValue();
		if (numCompressions == 0)
			numIterWithoutCompressions++;
		else
			numIterWithoutCompressions = 0;
		boolean keepGoing = (numIterWithoutCompressions < 2);

		if (keepGoing) {
			iter++;
		}
		else {
			Path outputPath = new Path(outputPathOrig.toString() + iter);

			// HEY!! 
			System.out.println("** renaming \"" + outputPath.toString() + "\" **");

			fileSystem.rename(outputPath, outputPathOrig);
		}

		return keepGoing;
}

	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {

		protected MRVertex createMRVertex(BytesWritable value) {
			return new MRVertex(value);
		}
		
		protected void map(IntWritable key, BytesWritable value, Context context) 
				throws IOException, InterruptedException {
			if (MRVertex.getIsBranch(value))
				throw new IOException("MRCompressChains.Mapper.map(): input vertex is a branch");

			MRVertex vertex = createMRVertex(value);
			
			// HEY!!
			System.out.println("* mapper output vertex " + vertex.getId() + 
					" source " + vertex.getIsSource() + " sink " + vertex.getIsSink() + " *");
			
			// HEY!! Error checking.
			IntWritable keyOut = new IntWritable(vertex.getCompressChainKey());
			context.write(keyOut, vertex.toWritable(MRVertex.EdgeFormat.EDGES_TO));
		}
	}

	// HEY!! What should the key be?
	public static class Reducer 
	extends org.apache.hadoop.mapreduce.Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {

		protected MRVertex createMRVertex(BytesWritable value) {
			return new MRVertex(value);
		}
		
		protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) 
				throws IOException, InterruptedException {
			MRVertex vertex1 = null;
			MRVertex vertex2 = null;
			
			for (BytesWritable value : values) {
				if (MRVertex.getIsBranch(value))
					throw new IOException("MRCompressChains.Reducer.reduce(): input vertex is a branch");

				// HEY!! Error checking
				if (vertex1 == null)
					vertex1 = createMRVertex(value);
				else
					vertex2 = createMRVertex(value);
			}
			
			if (vertex1 == null)
				throw new IOException("MRCompressChains.Reducer.reduce(): insufficient input vertices");
			
			// HEY!! The output key does not matter.
			
			if (vertex2 == null) {
				IntWritable keyOut = new IntWritable(vertex1.getId());
				context.write(keyOut, vertex1.toWritable(MRVertex.EdgeFormat.EDGES_TO));
			}
			else {
				int compressionKey = key.get();
				
				// HEY!!
				System.out.println("** compressing " + vertex2.toDisplayString() + " into " + vertex1.toDisplayString() 
						+ " key " + compressionKey + " **");
				
				MRVertex vertexCompressed = MRVertex.compressChain(vertex1, vertex2, compressionKey);
				
				if (vertexCompressed != null) {
					IntWritable keyOut = new IntWritable(vertexCompressed.getId());
					context.write(keyOut, vertexCompressed.toWritable(MRVertex.EdgeFormat.EDGES_TO));
				
					context.getCounter(CompressionCounter.numCompressions).increment(1);
				}
				else {
					// V1 -> V3 key V3, V2 -> V3 key V3, possible if V3 is a branch.
					IntWritable keyOut1 = new IntWritable(vertex1.getId());
					context.write(keyOut1, vertex1.toWritable(MRVertex.EdgeFormat.EDGES_TO));
					
					IntWritable keyOut2 = new IntWritable(vertex2.getId());
					context.write(keyOut2, vertex2.toWritable(MRVertex.EdgeFormat.EDGES_TO));
				}
			}
		}
	}

	private static enum CompressionCounter {
		numCompressions;
	}
	
	private static int iter;
	private static int numIterWithoutCompressions;
	
}
