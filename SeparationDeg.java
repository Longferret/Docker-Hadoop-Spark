package org.myorg;	
import java.io.IOException;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
	public class SeparationDeg {
		// From each line, outputs (title,actor)
		public static class Build_Graph_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    	private Text title = new Text();
			private Text actor = new Text();

	     	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       			String line = value.toString();
				String[] tokens = line.split("\t");
				// check if is an actor
				if(tokens[3].equals("actor")){
					title.set(tokens[0]);
					actor.set(tokens[2]);
					output.collect(title,actor);
				}
     		}
		}
		// From the key 'title' create group of actors (actor1,actor2), there are the edges of a graph
		public static class Build_Graph_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
			private Text actor1 = new Text();
			private Text actor2 = new Text();
     		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				ArrayList<String> actors = new ArrayList<String>();
				String received_actor;
      			while (values.hasNext()) {
					received_actor = values.next().toString();
					for(int i =0;i<actors.size();i++){
						actor1.set(received_actor);
						actor2.set(actors.get(i));
						output.collect(actor1,actor2);
					}
					actors.add(received_actor);

	       		}
	     	}
	   	}
		public static class BFS_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
			private Text actor1 = new Text();
			private Text out = new Text();
			private String infinity = "2147483647";
			// Could add dist = 0 when we are the known actor
	     	public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
       			String line = value.toString();
				String[] tokens = line.split("\t");
				String goat = "nm0000102";

				// -------- ACTOR1 -> ACTOR2 -----------
				actor1.set(tokens[1]);
				// If we are the goat, distance is 0
				if(tokens[1].equals(goat)){
					actor1.set(tokens[1]);
					out.set(tokens[0]+"\t"+"0");
					output.collect(actor1,out);
				}

				out.set(tokens[0]+"\t"+infinity);

				output.collect(actor1,out);
				
				// -------- ACTOR2 -> ACTOR1 -----------
				actor1.set(tokens[0]);
				// If we are the goat, distance is 0
				if(tokens[0].equals(goat)){
					out.set(tokens[1]+"\t"+"0");
				}

				out.set(tokens[1]+"\t"+infinity);

				output.collect(actor1,out);
     		}
	   	}

		public static class BFS_Iter_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
			private Text actor1 = new Text();
			private Text out = new Text();

	     	public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
       			String line = value.toString();
				String[] tokens = line.split("\t");
				if(tokens.length != 3){
					actor1.set(tokens[0]);
					out.set(tokens[1]);
					output.collect(actor1,out);
				}
				else{
					String current_actor = tokens[0];
					String adjacent_actor = tokens[1];
					String distance = tokens[2];
					// Just read data and send it
					actor1.set(current_actor);
					out.set(adjacent_actor+"\t"+distance);
					output.collect(actor1,out);
				}
     		}
	   	}
		public static class BFS_Iter_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
			public enum UpdateCounter {
				UPDATED
			} 
			private String infinity = "2147483647";
			private Text actor1 = new Text();
			private Text out = new Text();
     		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				ArrayList<String> adjacent_actors = new ArrayList<String>();
				String[] tokens;
				int r_val = -1;
				String r_adjacent_actors;
				int is_off = 0;
      			while (values.hasNext()) {
					tokens = values.next().toString().split("\t");
					// if no enough args, we already did this node, return
					if(tokens.length != 2){
						out.set(tokens[0]);
						output.collect(key,out);
						is_off = 1;
					}
					else{
						r_adjacent_actors = tokens[0];
						for(int i =0;i<adjacent_actors.size();i++){
							if(adjacent_actors.get(i).equals(r_adjacent_actors)){
								adjacent_actors.remove(i);
							}
						}
						// we received a value, we will spread it for next iteration (except for base)
						if(Integer.parseInt(tokens[1]) < 2147483647){
							r_val =Integer.parseInt(tokens[1]);
						}
						else{
							adjacent_actors.add(r_adjacent_actors);
						}
					}
	       		} // end while

				if(is_off != 1){
					if(r_val != -1 ){
						// report that a value is spread
						reporter.getCounter(UpdateCounter.UPDATED).increment(1);
						// spread the received value
						for(int i =0;i<adjacent_actors.size();i++){
							actor1.set(adjacent_actors.get(i));
							out.set(key.toString()+"\t"+String.valueOf(r_val+1));
							output.collect(actor1,out);
						}
						// shutdown this node (by putting 1 parameter in output)
						out.set(String.valueOf(r_val));
						output.collect(key,out);
					}
					// resend everything if we didn't get any distance
					else{
						for(int i =0;i<adjacent_actors.size();i++){
							out.set(adjacent_actors.get(i)+"\t"+infinity);
							output.collect(key,out);
						}
					}
				}

	     	}// end class
		}

		public static class BFS_Final_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
			private Text out = new Text();
			private String infinity = "2147483647";
			public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				String[] tokens;
				int is_set = 0;
				while (values.hasNext()) {
					tokens = values.next().toString().split("\t");
					if(tokens.length != 2){
						out.set(tokens[0]);
						output.collect(key,out);
						is_set = 1;
					}
				} // end while
				if(is_set == 0){
					out.set(infinity);
					output.collect(key,out);
				}
			} // end class
	   	}
	
	   	public static void main(String[] args) throws Exception {
			String input = args[0];
			String output = args[1];

			// -------PRE-PROCESSING JOB------
	     	JobConf conf = new JobConf(SeparationDeg.class);
     		conf.setJobName("Data Processing");
	     	conf.setOutputKeyClass(Text.class);
	     	conf.setOutputValueClass(Text.class);
	     	conf.setMapperClass(Build_Graph_Map.class);
	     	//conf.setCombinerClass(Reduce.class);
	    	conf.setReducerClass(Build_Graph_Reduce.class);
	    	conf.setInputFormat(TextInputFormat.class);
     		conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(input));
     		FileOutputFormat.setOutputPath(conf, new Path(output+"_edge"));
			JobClient.runJob(conf);
	
			// ----------INIT BFS JOB--------------

			conf = new JobConf(SeparationDeg.class);
			conf.setJobName("Init");
	     	conf.setOutputKeyClass(Text.class);
	     	conf.setOutputValueClass(Text.class);
	     	conf.setMapperClass(BFS_Map.class);
	    	conf.setInputFormat(TextInputFormat.class);
     		conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output+"_edge"));
     		FileOutputFormat.setOutputPath(conf, new Path(output+"_iter0"));
			JobClient.runJob(conf);
			// ----------BFS ITERATION JOB--------------

			RunningJob job;
			int i = 0;
			long counter = 0;
			do{
				conf = new JobConf(SeparationDeg.class);
				conf.setJobName("Iter");
	     		conf.setOutputKeyClass(Text.class);
	     		conf.setOutputValueClass(Text.class);
	     		conf.setMapperClass(BFS_Iter_Map.class);
				conf.setReducerClass(BFS_Iter_Reduce.class);
	    		conf.setInputFormat(TextInputFormat.class);
     			conf.setOutputFormat(TextOutputFormat.class);

				FileInputFormat.setInputPaths(conf, new Path(output+"_iter"+i));
     			FileOutputFormat.setOutputPath(conf, new Path(output+"_iter"+(i+1)));
				job =  JobClient.runJob(conf);
				counter = job.getCounters().findCounter(BFS_Iter_Reduce.UpdateCounter.UPDATED).getValue();
				i++;
			}
			while(0 != counter);


			// ----------FINAL REDUCE JOB---------- 
			conf = new JobConf(SeparationDeg.class);
			conf.setJobName("Final");
	     	conf.setOutputKeyClass(Text.class);
	     	conf.setOutputValueClass(Text.class);
	     	conf.setMapperClass(BFS_Iter_Map.class);
			conf.setReducerClass(BFS_Final_Reduce.class);
	    	conf.setInputFormat(TextInputFormat.class);
     		conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output+"_iter"+i));
     		FileOutputFormat.setOutputPath(conf, new Path(output+"_final"));
			JobClient.runJob(conf); 
   		}
	}