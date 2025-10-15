package csc369;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Sort {

    //mapper swaps the previous positions of (Country, Count) to sort
    //reducer puts them back

    public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
        private final IntWritable countAsKey = new IntWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int count = Integer.parseInt(value.toString());
            countAsKey.set(count);
            context.write(countAsKey, key);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text originalKey : values) {
                context.write(originalKey, key);
            }
        }
    }

    public static class DescendingIntWritableComparator extends WritableComparator {
        protected DescendingIntWritableComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -1 * a.compareTo(b);
        }
    }
}

