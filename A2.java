import java.io.*;
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


public class A2 
{
   
    public static class ProfitMapperClass extends Mapper<LongWritable,Text,Text,IntWritable>
       {
       
       
          public void map(LongWritable key, Text value, Context context)
          {             
             try{
                String[] str = value.toString().split(";");     
                int cost = Integer.parseInt(str[7]);
                int sales = Integer.parseInt(str[8]);
                int profit = sales-cost;
               
                context.write(new Text(str[5]), new IntWritable(profit));
             }
             catch(Exception e)
             {
                System.out.println(e.getMessage());
             }
          }
       }
   
      public static class ProfitReducerClass extends Reducer<Text,IntWritable,Text,IntWritable>
       {
            private IntWritable result = new IntWritable();
           
            public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
             int sum=0;
         
              for (IntWritable val : values)
                 {    
                     sum+=val.get();
                 }
                     
                    result.set(sum); 
                              
                
                
                 context.write(key, result);
              //context.write(key, new LongWritable(sum));
             
            }
       }
      
      public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            //conf.set("name", "value")
            //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
            Job job = Job.getInstance(conf, "Profit Transaction");
            job.setJarByClass(A2.class);
            job.setMapperClass(ProfitMapperClass.class);
            //job.setCombinerClass(HighValueReducerClass.class);
           job.setReducerClass(ProfitReducerClass.class);
           //job.setNumReduceTasks(0);
           //job.setMapOutputKeyClass(Text.class);
           //job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
          }
}
