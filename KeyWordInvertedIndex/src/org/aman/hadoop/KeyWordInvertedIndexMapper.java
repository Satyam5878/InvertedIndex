package org.aman.hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyWordInvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
	private Pattern pattern;
	public void setup(Context context) throws IOException, InterruptedException{
		pattern = Pattern.compile("\\{[^\\}]*\\}");
	}
	public void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException{
		try {
				Text newKey;
				Text newValue;
				String[] valueSplit = value.toString().split("\t\t");
				if(valueSplit.length == 2){
					Matcher matcher = pattern.matcher(valueSplit[1]);
					while(matcher.find()){
						StringBuilder valueStrBuilder = new StringBuilder(valueSplit[1].substring(matcher.start()+1,matcher.end()-1));
						valueSplit[1] = valueStrBuilder.toString();		
					}
					for(String split : valueSplit[1].split(" , ")){
						String[] wordANDcount = split.split(":");
						newValue = new Text(valueSplit[0]+":::"+wordANDcount[1]);
						newKey = new Text(wordANDcount[0]);
						context.write(newKey, newValue);
					}
					
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}

