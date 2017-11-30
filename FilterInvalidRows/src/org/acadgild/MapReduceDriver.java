package org.acadgild;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceDriver 
{

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception
	{
	
	Configuration conf = new Configuration();
	
	Job job = new Job(conf, "DataValidation");
    	job.setJarByClass(MapReduceDriver.class);
    	job.setMapperClass(MapperClass.class);
	job.setNumReduceTasks(0);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

