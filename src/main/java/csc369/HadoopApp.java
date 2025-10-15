package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("RequestCountPerCountry".equalsIgnoreCase(otherArgs[0])) {

		Path csvInput = new Path(args[1]);
		Path logInput = new Path(args[2]);
		Path finalOutputPath = new Path(args[3]);
		Path joinJobOutput = new Path("temp_join_output" );
		Path aggregateJobOutput = new Path("temp_aggregate_output");

		// --- JOB 1: Join logs with countries ---
		Configuration joinConf = new Configuration();
		joinConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		Job joinJob = Job.getInstance(joinConf, "Join Logs with Countries");

		MultipleInputs.addInputPath(joinJob, csvInput, KeyValueTextInputFormat.class, RequestCountPerCountry.CountryMapper.class);
		MultipleInputs.addInputPath(joinJob, logInput, TextInputFormat.class, RequestCountPerCountry.CountMapper.class);
		
		joinJob.setReducerClass(RequestCountPerCountry.JoinReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(joinJob, joinJobOutput);
		
		joinJob.waitForCompletion(true)

		//aggregate
		Configuration aggConf = new Configuration();
		Job aggregateJob = Job.getInstance(aggConf, "Aggregate Country Counts");
		aggregateJob.setMapperClass(CountryAggregator.AggregatorMapper.class);
		aggregateJob.setReducerClass(CountryAggregator.AggregatorReducer.class);
		aggregateJob.setOutputKeyClass(Text.class);
		aggregateJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(aggregateJob, joinJobOutput);
		FileOutputFormat.setOutputPath(aggregateJob, aggregateJobOutput);

		aggregateJob.waitForCompletion(true)

		//sort
		Configuration sortConf = new Configuration();
		Job sortJob = Job.getInstance(sortConf, "Sort Country Counts");
		sortJob.setJarByClass(HadoopApp.class);
		sortJob.setInputFormatClass(KeyValueTextInputFormat.class);

		sortJob.setMapperClass(Sort.SortMapper.class);
		sortJob.setReducerClass(Sort.SortReducer.class);
		sortJob.setSortComparatorClass(Sort.DescendingIntWritableComparator.class);
		
		sortJob.setMapOutputKeyClass(IntWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(sortJob, aggregateJobOutput);
		FileOutputFormat.setOutputPath(sortJob, finalOutputPath);

    	System.exit(sortJob.waitForCompletion(true) ? 0 : 1);


	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
