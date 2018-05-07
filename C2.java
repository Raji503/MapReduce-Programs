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
//use retail data D1,D2, D11,D12

public class C2 {  
    public static class C2MapperClass extends Mapper<LongWritable,Text,Text,Text>
       {
                  public void map(LongWritable key, Text value, Context context)
          {            
            
             try{
                String[] str = value.toString().split(";");    
              String proid=str[5];
              int sales=Integer.parseInt(str[8]);
              int cost=Integer.parseInt(str[7]);
              int loss = cost-sales;
              String age=str[2];
              String myvalue=age.trim()+','+String.format("%d", loss);
              context.write(new Text(proid), new Text(myvalue));
          }
             catch(Exception e)
             {
                System.out.println(e.getMessage());
             }
          }
       }
   
    public static class C2Partioner extends Partitioner<Text,Text>
    {
       // private IntWritable result = new IntWritable();
       
         public int getPartition(Text key, Text value,int numReduceTasks)
         {
                String[] str=value.toString().split(",");
                String age = str[0].trim();
           
                if(age.equals("A"))
                {
                    return 0;
                }
                if(age.equals("B"))
                {
                    return 1;
                }
                if(age.equals("C"))
                {
                    return 2;
                }
                if(age.equals("D"))
                {
                    return 3;
                }
                if(age.equals("E"))
                {
                    return 4;
                }
                if(age.equals("F"))
                {
                    return 5;
                }
                if(age.equals("G"))
                {
                    return 6;
                }
                if(age.equals("H"))
                {
                    return 7;
                }
                if(age.equals("I"))
                {
                    return 8;
                }
                if(age.equals("J"))
                {
                    return 9;
                }
                else
                {
                    return 10;
                }
         }
 
}
        
      public static class C2ReducerClass extends Reducer<Text,Text,Text,Text>
       {
          // private IntWritable result = new IntWritable();
          
            public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
                 
                long totalloss =0;
                String age = "";
               for(Text val:values)
               {              
                String[] token=val.toString().split(",");
                  totalloss = totalloss + Long.parseLong(token[1]);
                  age=token[0];
               }
               if(totalloss>0)
               {
                   String myvalue = age + ',' +String.format("%d", totalloss);
                   context.write(key, new Text(myvalue));
              //context.write(key, new LongWritable(sum));
                    }
            }
       }
    
      public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
                    //conf.set("name", "value")
            //conf.set("mapreduce.output.textoutputformat.separator", ",");
            Job job = Job.getInstance(conf, "Top viable products sold agewise");
            job.setJarByClass(C2.class);
            job.setMapperClass(C2MapperClass.class);
            job.setPartitionerClass(C2Partioner.class);
           //job.setCombinerClass(ReduceClass.class);
            job.setReducerClass(C2ReducerClass.class);
            job.setNumReduceTasks(11);
            job.setOutputKeyClass(Text.class);
            //job.setInputFormatClass(TextInputFormat.class);//binding data
            //job.setOutputFormatClass(SequenceFileOutputFormat.class);//not mandatory above two lines it ll not give op for all
           job.setMapOutputValueClass(Text.class);
           job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
          }
}
