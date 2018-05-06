import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



//use retail dataset D01,D02,D11,D12

public class C1
{
	public static class C1Mapperclass extends Mapper<LongWritable,Text,Text,Text>
	{	
//		private String prodid;
//		private String age;
//		private int profit;
//		private int sales;
//		private int cost;
		
		public void map(LongWritable key, Text value, Context context)
		{
						
			try
			{
				String[] str = value.toString().split(";");
	            String prodid = str[5];
	            int sales = Integer.parseInt(str[8]);
	            int cost = Integer.parseInt(str[7]);
	            int profit = sales - cost;
	            String age = str[2];
	            String myValue = age.trim() + ',' + String.format("%d", profit);
	            context.write(new Text(prodid), new Text(myValue));	
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}	
	public static class C1Partitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         String age = str[0].trim();
	         
	         if (age.equals("A"))
	         {
	        	 return 0; 
	         }
	         if(age.equals("B"))
	         {
	            return 1 ;
	         }
	         if(age.equals("C"))
	         {
	            return 2 ;
	         }
	         
	         if(age.equals("D"))
	         {
	            return 3 ;
	         }
	         
	         if(age.equals("E"))
	         {
	            return 4 ;
	         }
	         
	         if(age.equals("F"))
	         {
	            return 5 ;
	         }
	         
	         if(age.equals("G"))
	         {
	            return 6 ;
	         }
	         
	         if(age.equals("H"))
	         {
	            return 7 ;
	         }
	         
	         if(age.equals("I"))
	         {
	            return 8 ;
	         }
	         if(age.equals("J"))
	         {
	            return 9 ;
	         }	         
	         else
	         {
	            return 10;
	         }
	      }
	   }
	   

		
		public static class C1Reducerclass extends Reducer<Text,Text,Text,Text>
		{
			public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
			{
			
				long totalprofit=0;
				String age="";
			
					for(Text val:values)
					{
						String token[]=val.toString().split(",");
						age = token[0];
						totalprofit=totalprofit + Long.parseLong(token[1]);
					}
					if(totalprofit>0)
					{
						String myvalue=age+","+String.format("%d",totalprofit);
						context.write(key,new Text(myvalue));
					}	
						
			}	
		}
		
		public static void main(String args[]) throws Exception
		{
			Configuration conf=new Configuration();
			Job job=Job.getInstance(conf,"Top5 viable products");
			
			job.setJarByClass(C1.class);
			job.setMapperClass(C1Mapperclass.class);
			job.setPartitionerClass(C1Partitioner.class);
			job.setReducerClass(C1Reducerclass.class);
			job.setNumReduceTasks(11);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			System.exit(job.waitForCompletion(true)? 0:1);	
			
		}
}







