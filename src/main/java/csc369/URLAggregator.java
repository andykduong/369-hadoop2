package csc369;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;


public class URLAggregator {

    public static class AggregatorMapper extends Mapper<Text, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, one);
        }
    }
    
    public static class AggregatorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                System.out.println(val.get());
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
