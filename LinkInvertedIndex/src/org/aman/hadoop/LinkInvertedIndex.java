package org.aman.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class LinkInvertedIndex {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length < 2 ){
			System.out.println("Supply <inputlocation> <outputlocation");
			System.exit(1);
		}
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.seperator",",");
		//conf.set("key.value.seperator.in.input.line","\t\t");
		//conf.set("mapreduce.textoutputformat.seperator","\t\t");
	
		Job job = Job.getInstance(conf,"LinkInvertedIndex");
		job.setJarByClass(LinkInvertedIndex.class);
		job.setMapperClass(LinkInvertedIndexMapper.class);
		job.setReducerClass(LinkInvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
}
