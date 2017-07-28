package org.aman.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KeyWordInvertedIndex {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length < 2 ){
			System.out.println("Supply <inputlocation> <outputlocation");
			System.exit(1);
		}
		
		Job job = Job.getInstance(conf,"KeyWordInvertedIndex");
		job.setJarByClass(KeyWordInvertedIndex.class);
		job.setMapperClass(KeyWordInvertedIndexMapper.class);
		job.setReducerClass(KeyWordInvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
