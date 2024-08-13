package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AvgRating {
	// Outputs key(title) data(actor \t a) or key(title) data(rating \t r)
	public static class Rating_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// line to tokens 
			String line = value.toString();
			String[] tokens = line.split("\t", 10);
			if(tokens.length == 3){
				output.collect(new Text(tokens[0]), new Text(tokens[1]+"\tr"));
			}
			else{
				// if actor detected collect title(key) + actor
				if(tokens[3].equals("actor") || tokens[3].equals("actress")){
					output.collect(new Text(tokens[0]), new Text(tokens[2]+"\ta"));
				}
			}
		}
	}

	// From key(title) data(actor \t a/r) 
	// Outputs ACTOR RATING
	public static class Actor_Rating_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
			Vector<String> actors = new Vector<String>();
			String rating ="None";
			// put all actor in an array & save rating
			while (values.hasNext()) {
				String line = values.next().toString();
				String[] tokens = line.split("\t", 10);
				if(tokens[1].equals("r")){
					rating = tokens[0];
				}
				else{
					actors.addElement(tokens[0]);
				}
			}
			if(!rating.equals("None")){
				// collect all actors with their rating for that film
				for (int i=0;i<actors.size();i++){
					output.collect(new Text(actors.get(i)), new Text(rating));
				}
			}
		}
	}

	// Outputs key(ACTOR) data(RATING)
	public static class Average_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// line to tokens 
			String line = value.toString();
			String[] tokens = line.split("\t", 10);
			output.collect(new Text(tokens[0]), new Text(tokens[1]));
		}
	}

	// From key(ACTOR) data(RATING)
	// Outputs key(ACTOR) data(AVG_RATING)
	public static class Average_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
			double rating = 0;
			String r;
			double i = 0;
			// put all actor in an array
			while (values.hasNext()) {
				r = values.next().toString();
				if(!r.equals("None")){
					rating += Double.valueOf(r);
					i++;
				}
			}
			// collect actors + their average rating
			output.collect(key, new Text(Double.toString(rating/i)));
		}
	}

	public static void main(String[] args) throws Exception {
		 
		// Get rating per film & actors per film
		JobConf conf = new JobConf(AvgRating.class);
		conf.setJobName("Init");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Rating_Map.class);
		conf.setReducerClass(Actor_Rating_Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/Init"));
		JobClient.runJob(conf);
	

		// Calculate avg rating per actor 
		conf = new JobConf(AvgRating.class);
		conf.setJobName("Final");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Average_Map.class);
		conf.setReducerClass(Average_Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[1] + "/Init"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/Final"));

		JobClient.runJob(conf);
	}
}