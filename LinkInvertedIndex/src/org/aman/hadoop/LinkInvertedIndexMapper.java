package org.aman.hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LinkInvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text> {
	private Pattern pattern;
	public void setup(Context context) throws IOException, InterruptedException{
		//System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n               MAP_SETUP                       \n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		pattern = Pattern.compile("\\{[^\\}]*\\}");
		//context.write(new Text(),new Text());
	}
	public void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException{
		/*System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n               MAP                        \n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		String valueStr = value.toString().trim();
		Matcher matcher = pattern.matcher(valueStr);
		System.out.println(key.toString());
		while(matcher.find()){
			StringBuilder valueStrBuilder = new StringBuilder(valueStr.substring(matcher.start()+1,matcher.end()-1));
			//System.out.println(valueStrBuilder);
			valueStr = valueStrBuilder.toString();
					
		}
		
		for(String head_link : valueStr.split(" , ")){
			Text newKey = new Text(head_link.replace(":::","\t"));
			context.write(newKey, key);
		}
		*/
			try {
				Text newKey;
				Text newValue;
				String[] valueSplit = value.toString().split("\t\t");
				if(valueSplit.length == 2){
					newValue = new Text(valueSplit[0].replace("\t", ":::"));
					Matcher matcher = pattern.matcher(valueSplit[1]);
					while(matcher.find()){
						StringBuilder valueStrBuilder = new StringBuilder(valueSplit[1].substring(matcher.start()+1,matcher.end()-1));
						//System.out.println(valueStrBuilder);
						valueSplit[1] = valueStrBuilder.toString();		
					}
					for(String split : valueSplit[1].split(" , ")){
						newKey = new Text(split);
						context.write(newKey, newValue);
					}
					
				}
			} catch (StringIndexOutOfBoundsException e) {
				// TODO Auto-generated catch block
				System.out.println("Aman Catched Exception");
				e.printStackTrace();
			}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
