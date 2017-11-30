package org.acadgild;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper <LongWritable, Text, NullWritable, Text>
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		if(line.length()>0)
		{
			String[] fields = line.split("\\|");
			String companyName = fields[0];
			String productName = fields[1];

			if(!(companyName.equals("NA") || productName.equals("NA")))
			{		
				context.write(NullWritable.get(), new Text(value));
			}
	
		}
	}
}
