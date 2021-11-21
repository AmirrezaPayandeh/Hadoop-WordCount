import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCount
{
	public static void main(String[] args) throws Exception
	{
		if (args.length == 2)
		{
			for (int i = 0; i < args.length; i++)
				// Print input and output path (just for debugging)
				System.out.println(args[i]);
		}
		else
		{
			System.out.println("No Path Exists!");
			return; // Exit if no path is defined
		}
		
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("Word Count");
		
		// Set output key and output value format
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Set mapper and reducer classes
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		// Set input format and output format as text
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Set input and output paths
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		System.out.println("-----------------------Starting Map-Reduce-----------------------");
        long startTime = System.nanoTime();
		JobClient.runJob(conf);
		long endTime = System.nanoTime();
		System.out.println("-----------------------Map-Reduce Finished-----------------------");
		long timeElapsed = endTime - startTime;
        System.out.println("Total execution time for map-reduce in milliseconds: " + timeElapsed / 1000000);
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) // for each word in the line
			{
				word.set(tokenizer.nextToken());
				output.collect(word, one); // add it to the collection
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
	{
		Text[] ignoredWords =
		{
			new Text("a"), new Text("A"), new Text("an"), new Text("An"), new Text("the"), new Text("The"),
			new Text("I"), new Text("-"), new Text("We"), new Text("and"), new Text("as"), new Text("at"),
			new Text("by"), new Text("he"), new Text("she"), new Text("her"), new Text("his"), new Text("be"),
			new Text("to"), new Text("or"), new Text("on"), new Text("of"), new Text("my"), new Text("it"),
			new Text("is"), new Text("our"), new Text("their"), new Text("they"), new Text("was"), new Text("were"),
			new Text("we"), new Text("so"), new Text("not"), new Text("no"), new Text("your"), new Text("you"),
			new Text("this"), new Text("that"), new Text("This"), new Text("That"), new Text("may"), new Text("in"),
			new Text("have"), new Text("has"), new Text("had"), new Text("been"), new Text("for"), new Text("if"),
			new Text("can"), new Text("do"), new Text("It"), new Text("In"), new Text("are"), new Text("its"),
			new Text("with"), new Text("would"), new Text("will"), new Text("what"), new Text("when"), new Text("which"),
			new Text("who"), new Text("how"), new Text("any"), new Text("up")
		};
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			// words like a, an, we, she, is, or, and, etc. are ignored in the collection
			for (Text t : ignoredWords)
			{
				if (key.toString().equals(t.toString()))
					return;
			}
			
			int sum = 0;
			while (values.hasNext())
			{
				sum += values.next().get(); // count number of repeat
			}
			
			if (sum > 124000) // only collect words that are repeated more than 5000
			{
				output.collect(key, new IntWritable(sum));
			}
		}
	}
}