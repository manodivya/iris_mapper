package Iris;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import java.io.IOException;


public class IrisReducer  extends Reducer <Text,Text,Text,Text> {
   String[] tempString;
   float tempSepalLength, tempSepalWidth, tempPetalLength, tempPetalWidth;
   float totalSepalLength, totalSepalWidth, totalPetalLength,  totalPetalWidth;
   float minSepalLength, maxSepalLength, meanSepalLength, minSepalWidth, maxSepalWidth, meanSepalWidth, minPetalLength, maxPetalLength, meanPetalLength, minPetalWidth, maxPetalWidth, meanPetalWidth;
  

   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       

        String stringSL="";

        minSepalLength = minPetalLength = minSepalWidth = minPetalWidth = Float.MAX_VALUE;
        maxSepalLength = maxPetalLength = maxSepalWidth = maxPetalWidth = Float.MIN_VALUE;
    
          int count = 0;
          totalSepalLength =0;
          totalSepalWidth =0;
          totalPetalLength =0;
          totalPetalWidth =0;
       
       
        for(Text value: values) {

          

    


         // TODO use String split() method to split value and assign to tempString
         tempString = value.toString().split("_");

         // TODO convert tempString elements to temp sepal/petal length/width vars
          tempSepalLength = new Float(tempString[0]).floatValue();
          tempSepalWidth = new Float(tempString[1]).floatValue();
          tempPetalLength = new Float(tempString[2]).floatValue();
          tempPetalWidth = new Float(tempString[3]).floatValue();

         // TODO determine if you have min/max sepal/petal length/widths and assign to min/max sepal/petal lenght/widths accordingly

          if(tempSepalLength < minSepalLength)
          {
            minSepalLength = tempSepalLength;
          }
          if(tempSepalWidth < minSepalWidth)
          {
            minSepalWidth = tempSepalWidth;
          }
          if(tempPetalLength < minPetalLength)
          {
            minPetalLength = tempPetalLength;
          }
          if(tempPetalWidth < minPetalWidth)
          {
            minPetalWidth = tempPetalWidth;
          }

          if(tempSepalLength > maxSepalLength)
          {
            maxSepalLength = tempSepalLength;
          }
          if(tempSepalWidth > maxSepalWidth)
          {
            maxSepalWidth = tempSepalWidth;
          }
          if(tempPetalLength > maxPetalLength)
          {
            maxPetalLength = tempPetalLength;
          }
          if(tempPetalWidth > maxPetalWidth)
          {
            maxPetalWidth = tempPetalWidth;
          }

        // TODO calculate running totals for sepal/petal length/widths for use in calculation of means

          totalSepalLength = totalSepalLength + tempSepalLength;
          totalSepalWidth = totalSepalWidth + tempSepalWidth;
          totalPetalLength = totalPetalLength + tempPetalLength;
          totalPetalWidth = totalPetalWidth + tempPetalWidth;

         // TODO increment counter for use in calculation of means
           count = count + 1;

      } 

        // TODO calculate mean sepal/petal length/width 
        meanSepalLength = totalSepalLength/count;
        meanSepalWidth = totalSepalWidth/count;
        meanPetalLength = totalPetalLength/count;
        meanPetalWidth = totalPetalWidth/count;
      

      // TODO generate string output per the requirement
      // minSepalLength\tmaxSepalLength\tmeanSepalLength\t ...
    
      Text keyText = new Text(Float.toString(minSepalWidth) + "\t" +Float.toString(maxSepalWidth) + "\t" + Float.toString(meanSepalWidth) 
      + "\t" + Float.toString(minSepalLength) +"\t" + Float.toString(maxSepalLength) +"\t"+Float.toString(meanSepalLength) +"\t"+Float.toString(minPetalWidth)+"\t"+Float.toString(maxPetalWidth)+"\t"+Float.toString(meanPetalWidth)+"\t"+Float.toString(minPetalLength)+"\t"+Float.toString(maxPetalLength)+"\t"+Float.toString(meanPetalLength));
      // TODO emit output to context
        
        context.write(key, keyText);
      
    
     

   }
}
