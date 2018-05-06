import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class stdcalldur
{
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		  Text phonenumber = new Text();
		  IntWritable durationInMinutes= new IntWritable();
		  
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");
	            if(str[4].equals("1"))
	            {
	            	phonenumber.set(str[0]);
	            	String callendtime=str[3];
	            	String callstarttime=str[2];
	            	long duration=toMillis(callendtime)-toMillis(callstarttime);
	            	durationInMinutes.set(((int)(duration/(1000*60))));
	            	context.write(phonenumber,durationInMinutes);
	            }
	            
	         	}
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	       }
	      
	      public long toMillis(String date)
	      {
	    	  SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
	    	  Date datefrm = null;
	    	  try
	    	  {
	    		  datefrm=format.parse(date);
	    	  }
	    	  catch (ParseException e)
	    	  {
	    		  e.printStackTrace();
	    	  }
	    	  return datefrm.getTime();
	      }
	     }
	   
	
	  public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		    private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		    {
		    	int sum=0;
		    	for(IntWritable val:values)
		    	{
		    		sum=sum+val.get();
		    	}
		    		if(sum>=60)
		    		{
		    			result.set(sum);
		    			context.write(key,result);
		    		}
		    	
		    		
		     }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformat.seperator",",");
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "stdcalls");
		    job.setJarByClass(stdcalldur.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    //job.setMapOutputKeyClass(Text.class);
		    //job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}

