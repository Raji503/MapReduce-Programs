import java.io.IOException;

import org.apache.hadoop.conf. Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io. LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce. Job;
import org.apache.hadoop.mapreduce. Mapper;
import org.apache.hadoop.mapreduce. Reducer;
import org.apache.hadoop.mapreduce. lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce. lib.output.FileOutputFormat;
import org.apache.hadoop.io. DoubleWritable;
//import org.apache.hadoop.mapreduce. Mapper;
//import org.apache.hadoop.mapreduce. Reducer;
//use retail data D1,D2, D11,D12

public class A3 {   
    public static class ProfitValueMapperClass extends Mapper<LongWritable,Text,Text, Text>
       {
                  public void map(LongWritable key, Text value, Context context)
          {             
             
             try{
                String[] str = value.toString().split(";");      
              String cost_sales=str[7]+','+str[8];         
               context.write(new Text(str[5]), new Text(cost_sales));
          }
             catch(Exception e)
             {
                System.out.println(e. getMessage());
             }
          }
       }
   
      public static class ProfitValueReducerClass extends Reducer<Text,Text,Text, DoubleWritable>
       {
          // private IntWritable result = new IntWritable();
           
            public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
                   long sales=0;
                 long cost=0;
               long  total_cost=0;
               long total_sales=0;
               long profit=0;
               for(Text val:values)
               {               
                String[] token=val.toString().split("," );
                   cost=Long.parseLong(token[0]);
               sales=Long.parseLong(token[1]) ;
               total_cost+=cost;
               total_sales+=sales;   
               }
               if(total_cost==0)//no infinty variable
               total_cost=1;
               profit= (total_sales-total_cost);
               double profitpercent=((double)profit* 100/(double)total_cost);
                  
               //String myValue= dt +','+custid+','+String.format( "%d", maxSales);       
                       
                       
                     context.write(key, new DoubleWritable(profitpercent)) ;
              //context.write(key, new LongWritable(sum));
                    }
            }
     
      public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
                    //conf.set("name", "value")
            conf.set("mapreduce.output. textoutputformat.separator", ",");
            Job job = Job.getInstance(conf, "Profit Margin");
            job.setJarByClass(A3.class);
            job.setMapperClass( ProfitValueMapperClass.class);
            //job.setCombinerClass( ReduceClass.class);
            job.setReducerClass( ProfitValueReducerClass.class) ;
            //job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text. class);
            //job.setInputFormatClass( TextInputFormat.class);// binding data
            //job.setOutputFormatClass( SequenceFileOutputFormat. class);//not mandatory above two lines it ll not give op for all
           job.setMapOutputValueClass( Text.class);
           job.setMapOutputKeyClass(Text. class);
            job.setOutputValueClass( DoubleWritable.class);
            FileInputFormat.addInputPath( job, new Path(args[0]));
            FileOutputFormat. setOutputPath(job, new Path(args[1]));
            System.exit(job. waitForCompletion(true) ? 0 : 1);
          }
}
