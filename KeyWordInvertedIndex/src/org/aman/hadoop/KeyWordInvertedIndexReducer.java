package org.aman.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyWordInvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key ,Iterable<Text> values, Context context) throws IOException, InterruptedException{
		StringBuilder sb = null;
		try {
			sb = new StringBuilder("[{");
			for(Text value : values){
				sb.append(value.toString()+" , ");
			}
			sb.replace(sb.length()-2,sb.length(), "");
			sb.append("}]");
			context.write(key,new Text(sb.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
