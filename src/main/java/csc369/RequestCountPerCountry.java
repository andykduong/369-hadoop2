package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestCountPerCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    String country = value.toString();
	    String out = "country-" + country;
	    context.write(key, new Text(out));
	} 
    }

    // Mapper for messages file
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    String parts[] = value.toString().split(" ");
	    String hostname = parts[0];
		context.write(new Text(hostname), new Text("one"));
	    }
	}
    


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
	    String country = "Unknown Location";
		int count = 0;

		for (Text val : values) {
			String value = val.toString();
			if (value.startsWith("country-")) {
				country = value.substring("country-".length()) + ",";
			} else {
				if (value.equals("one")) {
					count++;
				}
			}
		}

		if (count > 0 && !country.equals("Unknown Location")) {
			String countString = Integer.toString(count);
			context.write(new Text(country), new Text(countString));
		}
	}
    } 

}