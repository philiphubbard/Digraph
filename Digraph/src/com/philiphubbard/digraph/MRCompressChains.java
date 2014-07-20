package com.philiphubbard.digraph;

import com.philiphubbard.digraph.MRVertex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRCompressChains {

	// HEY!! Use SequenceFileOutputFormat as a more efficient intermediate format between
	// multiple map-reduce phases?
	
	public static void beginIteration() {
		iter = 0;
		numIterWithoutMerges = 0;
	}
	
	public static void setupIterationJob(Job job, Path inputPathOrig, Path outputPathOrig)
			throws IOException {
		job.setJarByClass(MRCompressChains.class);
		job.setMapperClass(MRCompressChains.Mapper.class);
		job.setCombinerClass(MRCompressChains.Reducer.class);
		job.setReducerClass(MRCompressChains.Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		Path inputPath;
		if (iter == 0)
			inputPath = inputPathOrig;
		else
			inputPath = new Path(outputPathOrig.toString() + (iter - 1));
		Path outputPath = new Path(outputPathOrig.toString() + iter);
		
		KeyValueTextInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// HEY!! 
		System.out.println("** iteration " + iter + " input \"" + inputPath.toString()
				+ "\" output \"" + outputPath.toString() + "\" **");
	}
	
	// HEY!! Keep this around, for more control?  Or eliminate, now that other is there?
	public static void setupJob(Job job, Path inputPath, Path outputPath)
			throws IOException {
		job.setJarByClass(MRCompressChains.class);
		job.setMapperClass(MRCompressChains.Mapper.class);
		job.setCombinerClass(MRCompressChains.Reducer.class);
		job.setReducerClass(MRCompressChains.Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);	
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
		long numMerges = jobCounters.findCounter(MRCompressChains.MergeCounter.numMerges).getValue();
		if (numMerges == 0)
			numIterWithoutMerges++;
		else
			numIterWithoutMerges = 0;
		boolean keepGoing = (numIterWithoutMerges < 2);

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
	// HEY!! Old, before using KeyValueTextInputFormat
	// extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {
	extends org.apache.hadoop.mapreduce.Mapper<Text, Text, IntWritable, Text> {

		protected MRVertex createMRVertex(Text value) {
			return new MRVertex(value);
		}
		
		// HEY!! Old, before using KeyValueTextInputFormat
		//protected void map(LongWritable key, Text value, Context context) 
		protected void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			if (MRVertex.getIsBranch(value))
				throw new IOException("MRCompressChains.Mapper.map(): input vertex is a branch");

			MRVertex vertex = createMRVertex(value);
			
			// HEY!! Error checking.
			IntWritable keyOut = new IntWritable(vertex.getMergeKey());
			context.write(keyOut, vertex.toText(MRVertex.EdgeFormat.EDGES_TO));
		}
	}

	// HEY!! What should the key be?
	public static class Reducer 
	extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

		protected MRVertex createMRVertex(Text value) {
			return new MRVertex(value);
		}
		
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			MRVertex vertex1 = null;
			MRVertex vertex2 = null;
			
			for (Text value : values) {
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
				context.write(keyOut, vertex1.toText(MRVertex.EdgeFormat.EDGES_TO));
			}
			else {
				int mergeKey = key.get();
				
				// HEY!!
				System.out.println("** merging " + vertex2.toDisplayString() + " into " + vertex1.toDisplayString() 
						+ " key " + mergeKey + " **");
				
				MRVertex vertexMerged = MRVertex.merge(vertex1, vertex2, mergeKey);
				
				if (vertexMerged.getId() != MRVertex.NO_VERTEX) {
					IntWritable keyOut = new IntWritable(vertexMerged.getId());
					context.write(keyOut, vertexMerged.toText(MRVertex.EdgeFormat.EDGES_TO));
				
					context.getCounter(MergeCounter.numMerges).increment(1);
				}
				else {
					// V1 -> V3 key V3, V2 -> V3 key V3, possible if V3 is a branch.
					IntWritable keyOut1 = new IntWritable(vertex1.getId());
					context.write(keyOut1, vertex1.toText(MRVertex.EdgeFormat.EDGES_TO));
					
					IntWritable keyOut2 = new IntWritable(vertex2.getId());
					context.write(keyOut2, vertex2.toText(MRVertex.EdgeFormat.EDGES_TO));
				}
			}
		}
	}

	private static enum MergeCounter {
		numMerges;
	}
	
	private static int iter;
	private static int numIterWithoutMerges;
	
}
