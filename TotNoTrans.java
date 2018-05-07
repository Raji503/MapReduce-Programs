//Reduce side join Example
//Find  the total number of transaction done by each customer and the value of those transaction
// Files:custs.txt,txns1.txt

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotNoTrans {
   
    public static class CustMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String valueArr[] = value.toString().split(",");
            String custID = valueArr[0];
            String custFname = valueArr[1];
            String c_custdetails = "c" + "," + custFname;
            context.write(new Text(custID), new Text(c_custdetails));
        }
    }

    public static class StoreMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String strValue = value.toString();
            String[] valueArr = strValue.split(",");
            String custId = valueArr[2];
            String amt = valueArr[3];
            String s_price = "s" + "," + amt;
            context.write(new Text(custId), new Text(s_price));
        }
    }

    public static class MyReducer1 extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text> value, Context context) throws IOException, InterruptedException
        {
            //int occCount = 0;
            String occ = "unknown";
           
            double tot = 0.0;
            int count=0;
            //String res = null;
            for(Text val : value)
            {
                String valArr[] = val.toString().split(",");
                String marker = valArr[0];
                if(marker.equals("s"))
                {
                    double Price = Double.parseDouble(valArr[1]);
                    //occCount++;
                    tot+=Price;
                    count++;
                }
                else if(marker.equals("c"))
                {
                    occ = valArr[1];
                   
                   
                }
            }
            String tvalue=String.format("%f", tot);
            String tcount=String.format("%d", count);
            String fvalue=tcount+','+tvalue;
           
            //res = String.valueOf(occCount) + "--" + String.valueOf(tot);
            context.write(new Text(occ), new Text(fvalue));
        }
    }
   

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf,"Occupation - totaltxn ");
        job1.setJarByClass(TotNoTrans.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, StoreMapper.class);
        //Path outputPath1 = new Path("FirstMapper");
       
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        FileSystem.get(conf).delete(outputPath1, true);
       
       
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
       
   
    }
       
    }

