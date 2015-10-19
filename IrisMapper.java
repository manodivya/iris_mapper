package Iris;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class IrisMapper  extends Mapper <LongWritable,Text,Text,Text> {
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
       // TODO create array of string tokens from record assuming space-separated fields using split() method of String class
       String[] s = value.toString().split("\\s+");
       // TODO pull out sepal length from columns var
       String sepal_length = s[0];
       //  TODO pull out sepal width from columns var
       String sepal_width = s[1];
       // TODO pull out petal length from columns var
       String petal_length = s[2];
       // TODO pull out petal width from columns var
       String petal_width = s[3];
       // TODO pull out flower id from columns var

       String flowerId = s[4];

       // TODO write output to context as key-value pair where key is 
      // flowerId and value is underscore-separated concatenation of 
      // sepal/petal length/widths
       context.write(new Text(flowerId), new Text(sepal_length+"_"+sepal_width+"_"+petal_length+"_"+petal_width));

       


      

      

      

      


     
     
   }
}
