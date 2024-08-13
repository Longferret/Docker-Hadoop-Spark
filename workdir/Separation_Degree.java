package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Separation_Degree {
	public static enum MyCounters {
        NODE_COUNTER
    }

	// The first is Kevin Bacon, the second is Meryl Streep
	// We can add any number of actor to this list to compute any number of hops from these actors to all other actors
	// But the time complexity is O(n) where n is the number of actors
	//public static final String[] actors_separation = {"nm3636162", "nm0000658"};
	public static final String[] actors_separation = {"nm0000658"};

	// Outputs key(sep_actor+title) data(actor+Distance)
	// The distance is infinity except for the searched actor
	public static class Init_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// line to tokens 
			String line = value.toString();
			String[] tokens = line.split("\t", 10);

			// if actor detected collect title(key) + actor
			if(tokens[3].equals("actor") || tokens[3].equals("actress")){
				for(int i =0;i<actors_separation.length;i++){
					if(tokens[2].equals(actors_separation[i])){
						output.collect(new Text(actors_separation[i]+"\t"+tokens[0]), new Text(tokens[2]+"\t0"));
					}
					else{
						output.collect(new Text(actors_separation[i]+"\t"+tokens[0]), new Text(tokens[2]+"\t2147483647"));
					}
				}
			}
		}
	}

	// Outputs key(sep_actor+title) data(actor+Distance)
	public static class Algo1_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// line to tokens 
			String line = value.toString();
			String[] tokens = line.split("\t", 10);
			output.collect(new Text(tokens[0]+"\t"+tokens[1]), new Text(tokens[2]+"\t" + tokens[3]));
		}
	}

		// Outputs key(sep_actor+actor) data(title+Distance)
	public static class Algo2_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// line to tokens 
			String line = value.toString();
			String[] tokens = line.split("\t", 10);
			output.collect(new Text(tokens[0]+"\t"+tokens[2]), new Text(tokens[1]+"\t" + tokens[3]));
		}
	}

	// From key(sep_actor+title) data(actor+Distance)
	public static class Algo1_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
			Vector<String> actors = new Vector<String>();
			String[] tokens;
			int dist;
			int non_infinite_val = -1;

			// Case when the actor has already its distance
			String title = key.toString().split("\t", 10)[1];
			if(title.equals("_END_")){
				while (values.hasNext()) {
					output.collect(key, values.next());
				}
				return;
			}

			// put all actor/distances in an array
			while (values.hasNext()) {
				tokens = values.next().toString().split("\t", 10);
				dist = Integer.valueOf(tokens[1]);
				if(dist < 2147483647){
					non_infinite_val = dist;
				}
				else{
					actors.addElement(tokens[0]);
				}
			}

			for (int i=0;i<actors.size();i++){
				// We spread the value
				if(non_infinite_val!=-1){
					output.collect(key, new Text(actors.get(i)+"\t" + Integer.toString(non_infinite_val+1)));
				}
				else{
					output.collect(key, new Text(actors.get(i)+"\t" + "2147483647"));
				}

			}
		}
	}
	// From key(sep_actor+actor) data(title+Distance)
	public static class Algo2_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
			Vector<String> titles = new Vector<String>();
			String[] tokens;
			String[]tokens2 = key.toString().split("\t", 10);
			String actor = tokens2[1];
			String sep_actor = tokens2[0];
			int dist;
			int non_infinite_val = -1;
			// put all titles/distances in an array
			while (values.hasNext()) {
				tokens = values.next().toString().split("\t", 10);
				dist = Integer.valueOf(tokens[1]);
				if(dist < 2147483647){
					non_infinite_val = dist;
				}
				else{
					titles.addElement(tokens[0]);
				}
			}
			
			for (int i=0;i<titles.size();i++){
				// Spread the actor distance to all its films
				if(non_infinite_val!=-1){
					reporter.getCounter(Separation_Degree.MyCounters.NODE_COUNTER).increment(1);
					output.collect(new Text(sep_actor+"\t"+titles.get(i)), new Text(actor + "\t" + Integer.toString(non_infinite_val)));
				}
				// Just resend infinity
				else{
					output.collect(new Text(sep_actor+"\t"+titles.get(i)), new Text(actor + "\t" + "2147483647"));
				}

			}
			// Send the actor distance with special keyword
			if(non_infinite_val != -1){
				output.collect(new Text(sep_actor+"\t_END_"), new Text(actor + "\t" + Integer.toString(non_infinite_val)));
			}
		}
	}

	// Outputs
	public static class Final_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
			String[] tokens;
			int gone = 0;
			while (values.hasNext()) {
				tokens = values.next().toString().split("\t", 10);
				if(gone == 0){
					output.collect(key, new Text(tokens[1]));
					gone=1;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		 
		// Init
		JobConf conf = new JobConf(Separation_Degree.class);
		JobConf conf2;
		conf.setJobName("Init");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Init_Map.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/Iter0"));

		JobClient.runJob(conf);
		
		int i = 0;
		RunningJob j;
		long cc = 0;

		// BFS
		do{
			// Algo part1
			conf = new JobConf(Separation_Degree.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Algo1_Map.class);
			conf.setReducerClass(Algo1_Reduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(args[1] + "/Iter"+Integer.toString(i)+"/part-00000"));
			FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/Iter"+Integer.toString(i+1)));
			JobClient.runJob(conf);

			// Algo part2
			conf2 = new JobConf(Separation_Degree.class);
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);
			conf2.setMapperClass(Algo2_Map.class);
			conf2.setReducerClass(Algo2_Reduce.class);
			conf2.setInputFormat(TextInputFormat.class);
			conf2.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf2, new Path(args[1] + "/Iter"+Integer.toString(i+1)+"/part-00000"));
			FileOutputFormat.setOutputPath(conf2, new Path(args[1] + "/Iter"+Integer.toString(i+2)));
			j = JobClient.runJob(conf2);
			cc = j.getCounters().findCounter(MyCounters.NODE_COUNTER).getValue();

			i+=2;
		}
		while(cc != 0);
		//while(i < 0);

		// Final
		conf = new JobConf(Separation_Degree.class);
		conf.setJobName("Final");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Algo2_Map.class);
		conf.setReducerClass(Final_Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[1] + "/Iter"+Integer.toString(i)+"/part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/Final"));

		JobClient.runJob(conf);
		


	}
}