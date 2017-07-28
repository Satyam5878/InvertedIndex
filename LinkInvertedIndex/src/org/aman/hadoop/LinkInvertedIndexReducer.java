package org.aman.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LinkInvertedIndexReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key ,Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n               MAP                        \n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		StringBuilder sb = new StringBuilder("[");
		for(Text value : values){
			sb.append(value.toString()+" , ");
		}
		sb.replace(sb.length()-2,sb.length(), "");
		sb.append("]");
		context.write(key, new Text(sb.toString()));
	}
}


