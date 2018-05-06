import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class A1
{

	public static class HighValueMapperClass extends Mapper<LongWritable,Text,Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
	String[] str = value.toString().split(";");
 String mykey = "common";
 String dt = str[0];
 String custid = str[1].trim();
 String sales = str[8];
 String myValue = dt +','+custid + ','+ sales;
 context.write(new Text(mykey),new Text(myValue));
 }
catch(Exception e)
{
	System.out.println(e.getMessage());
	}
		}
	}

public static class HighValueReducerClass extends Reducer<Text,Text, NullWritable,Text>
{
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		int sales=0;
		int maxSales=0;
		String dt = "";
		String custid = "";
		for (Text val : values)
		{ 
		String[] token = val.toString().split(",");
		sales = Integer.parseInt(token[2]);
		if(sales > maxSales)
		{
		maxSales=sales;
		dt = token[0];
		custid = token[1];
		sales = 0;
		}
		if(sales == maxSales)
		{
		dt = dt +','+token[0];
		custid = custid + ',' + token[1];
		}
		}
		String myValue = dt + ',' + custid + ','+String.format("%d", maxSales);
		context.write(NullWritable. get(), new Text(myValue));
		//context.write(key, new LongWritable(sum));
		}

		}
		public static void main(String[] args) throws Exception 
		{
		Configuration conf = new Configuration();
		//conf.set("name", "value")
		//conf.set("mapreduce.input. fileinputformat.split.minsize" , "134217728");
		Job job = Job.getInstance(conf,"Highest single value transaction");
		job.setJarByClass(A1.class);
		job.setMapperClass(HighValueMapperClass.class);
		//job.setCombinerClass( HighValueReducerClass.class);
		job.setReducerClass(HighValueReducerClass.class);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
	}
		
