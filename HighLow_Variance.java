import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class HighLow_Variance {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		 private Text stock_id = new Text();
		 private Text HighLow = new Text();
		 
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            String highlow = str[4] + ',' + str[5];
	            stock_id.set(str[1]);
	            HighLow.set(highlow);
	            
	            //context.write(new Text(str[1]),new LongWritable(vol));
	            context.write(stock_id, HighLow);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,DoubleWritable>
	   {
		    private DoubleWritable result = new DoubleWritable();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
				double maxValue=0.00;
//				double min_temp=0.00;
//				double max_temp=0.00;
				double minValue = 10000.00;
				double variance = 0.00;
				
				for (Text t : values) 
				{
					String str[] = t.toString().split(",");
					double max_temp = Double.parseDouble(str[0]);
					double min_temp = Double.parseDouble(str[1]);
					
					if (max_temp > maxValue) 
					{
						maxValue = max_temp;
					}
					
					if (min_temp < minValue) 
					{
						minValue = min_temp;
					}
				}
				variance = ((maxValue-minValue)*100)/minValue;
				
				result.set(variance);

		      context.write(key, result);
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Variance for each stock between high and low");
		    job.setJarByClass(HighLow_Variance.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
