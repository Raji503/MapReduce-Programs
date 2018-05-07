import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//use retail data D11,D12,D01 and D02


public class B {

	   public static class BMapperClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            long sales = Long.parseLong(str[8]);
	            context.write(new Text(str[5].trim()), new LongWritable(sales));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   
	   public static class BReducerClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {

	      public void reduce(Text key, Iterable <LongWritable> values, Context context) throws IOException, InterruptedException
	      {
	         long sum = 0;
	         for (LongWritable val : values)
		         {
		         	sum += val.get();
		         	 
		         }
			context.write(key, new LongWritable(sum));
	      }
	   }
	   
		public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
		{
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] valueArr = value.toString().split("\t");
				context.write(new LongWritable(Long.parseLong(valueArr[1])), new Text(valueArr[0]));
			}
		}

		public static class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable>
		{
			int counter=1;
			public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
			{
				for(Text val : value)
				{
					if (counter<11)
					{
					context.write(new Text(val), key);
					counter = counter+1;
					}
				}
			}
		}
		
	   

//Main class
	   
	   public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Total sales calc for each product");
		    job.setJarByClass(B.class);
		    job.setMapperClass(BMapperClass.class);
		    job.setReducerClass(BReducerClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
			Path outputPath1 = new Path("FirstMapper");
			FileSystem.get(conf).delete(outputPath1, true);
			FileOutputFormat.setOutputPath(job, outputPath1);
			job.waitForCompletion(true);

			Job job2 = Job.getInstance(conf,"Top 10 Sorting on Highest Sales");
			job2.setJarByClass(B.class);
			job2.setMapperClass(SortMapper.class);
			job2.setReducerClass(SortReducer.class);
			job2.setSortComparatorClass(DecreasingComparator.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job2, outputPath1);
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			FileSystem.get(conf).delete(new Path(args[1]), true);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		  }
}
