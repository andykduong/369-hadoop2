// URLSort.java
package csc369;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class URLSort {

    public static class CountryCountWritable implements WritableComparable<CountryCountWritable> {
        private Text country;
        private IntWritable count;

        public CountryCountWritable() {
            this.country = new Text();
            this.count = new IntWritable();
        }

        public void set(String country, int count) {
            this.country.set(country);
            this.count.set(count);
        }
        
        public Text getCountry() {
            return country;
        }

        public IntWritable getCount() {
            return count;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            country.readFields(in);
            count.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            country.write(out);
            count.write(out);
        }

        @Override
        public int compareTo(CountryCountWritable other) {
            int countryCompare = this.country.compareTo(other.country);
            //if countries are not the same sort by country
            if (countryCompare != 0) {
                return countryCompare;
            }
            // If countries are the same sort by count (descending)
            return other.count.compareTo(this.count);
        }
    }

    public static class SortMapper extends Mapper<Text, Text, CountryCountWritable, Text> {
        private CountryCountWritable mapOutputKey = new CountryCountWritable();
        private Text mapOutputValue = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            int lastSpaceIndex = line.lastIndexOf(' ');

            String country = line.substring(0, lastSpaceIndex).trim();
            String url = line.substring(lastSpaceIndex).trim();
            int count = Integer.parseInt(value.toString());

            mapOutputKey.set(country, count);
            mapOutputValue.set(url);
            
            context.write(mapOutputKey, mapOutputValue);
        }
    }

    public static class CountryPartitioner extends Partitioner<CountryCountWritable, Text> {
        @Override
        public int getPartition(CountryCountWritable key, Text value, int numPartitions) {
            return (key.getCountry().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    public static class CountryGroupingComparator extends WritableComparator {
        protected CountryGroupingComparator() {
            super(CountryCountWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CountryCountWritable key1 = (CountryCountWritable) a;
            CountryCountWritable key2 = (CountryCountWritable) b;
            return key1.getCountry().compareTo(key2.getCountry());
        }
    }


    public static class SortReducer extends Reducer<CountryCountWritable, Text, Text, Text> {
        private Text outputKey = new Text();

        @Override
        public void reduce(CountryCountWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text url : values) {
                String countryAndUrl = key.getCountry().toString() + "\t" + url.toString();
                outputKey.set(countryAndUrl);
                context.write(outputKey, new Text(key.getCount().toString()));
            }
        }
    }
}