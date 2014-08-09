// Simple Map reduce program to map word sizes to number of occurrences of that word size
// Assumes a space delimiter
// Input: Hey, I will go to eat now 
// Output: Map[] <k, v> = [<1,1> (I) <2, 2> (go, to) <3, 3> (will, eat, now) <4, 1> (Hey,) ] 
// Vishal K

package com.myorg;
	
import java.io.IOException;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class frequency_counter {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
	     private final static IntWritable one = new IntWritable(1);
	     private IntWritable count = new IntWritable();
	
	     public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
	    	 String line = value.toString();
	    	 StringTokenizer tokenizer = new StringTokenizer(line);
	    	 while (tokenizer.hasMoreTokens()) {
	    		 String word = tokenizer.nextToken();
	    		 count.set(word.length());
	    		 output.collect(count, one);
	    	 }
	     }
   }
	
   public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	   public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
		   int sum = 0;
	       while (values.hasNext()) {
	    	   sum += values.next().get();
	       }
	       output.collect(key, new IntWritable(sum));
	   }
   }
	
   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(frequency_counter.class);
	     conf.setJobName("frequencycount");
	
	     conf.setOutputKeyClass(IntWritable.class);
	     conf.setOutputValueClass(IntWritable.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	   }
}