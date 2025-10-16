package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.naming.Context;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryURLCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for hostname_country file
    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    String country = value.toString();
	    String out = "country-" + country;
	    context.write(key, new Text(out));
		} 
    }

    // Mapper for access log
    public static class IP_URLMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    String parts[] = value.toString().split(" ");
	    String hostname = parts[0];

		String parts2[] = value.toString().split("\"");
		String request = parts2[1];
		String parts3[] = request.split(" ");
		String url = parts3[1];
		context.write(new Text(hostname), new Text("url-" + url));
	    }
	}

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
	    String country = "Unknown Location";
		List<String> urls = new ArrayList<>();

		for (Text val : values) {
			String id = val.toString();
			if (id.startsWith("country-")) {
				country = id.substring("country-".length());
			} else if(id.startsWith("url-")) {
				urls.add(id.substring(4));
			}
		}
		if (!urls.isEmpty() && !country.equals("Unknown Location")) {
			for (String url : urls){
				context.write(new Text(country + " " + url), one);
			}
		}
	}
    } 

}