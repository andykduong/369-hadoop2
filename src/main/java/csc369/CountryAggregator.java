package csc369;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.io.LongWritable;


public class CountryAggregator {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // mapper reads the output of the previous job (Country  Count)
    // and emits (Count, Country) so the reducer can sum them
    public static class AggregatorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable countWritable = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String text[] = value.toString().split(",");
            int count = Integer.parseInt(text[1].toString().trim());
            countWritable.set(count);
            context.write(new Text(text[0]), countWritable);
        }
    }

    public static class AggregatorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
