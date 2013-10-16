//Class contains modified code from Cloud9. Information about Cloud9 can be 
//found in the comment block below this one.

/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */





import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cloud9.ArrayListWritable;
import cloud9.Histogram;
import cloud9.MapKI;
import cloud9.PairOfInts;


public class BuildInvertedIndex extends Configured implements Tool {
	

	private static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, PairOfInts> {
		

		private static final Text word = new Text();
		private Histogram<String> termCounts = new Histogram<String>();		
		
		
		//This mapreduce job is taking in a document, then outputting the count for each word in the document
		//Input is document
		//output is (word,  PairOfInts(documentOffset, countOfTheWord))
		public void map(LongWritable docno, Text doc, OutputCollector<Text, PairOfInts> output,
			Reporter reporter) throws IOException {
			
			String text = doc.toString();

            termCounts.clear();			
			String[] terms = text.split("\\s+");
			
			
			//This word counting method is from 
			//http://stackoverflow.com/questions/14594102/java-most-suitable-data-structure-for-finding-the-most-frequent-element
			//written by maerics Jan 23, 2013
			//It is figuring out the max term frequency of a given document the hard way.
			Integer maxCount = -1;
			Map<String, Integer> wordCount = new HashMap<String, Integer>();
			for (String str : terms) {
			  if (!wordCount.containsKey(str)) { wordCount.put(str, 0); }
			  int count = wordCount.get(str) + 1;
			  if (count > maxCount) {
			    maxCount = count;
			  }
			  wordCount.put(str, count);
			}
			
			
			
			// first build a histogram of the terms
			for (String term : terms) {
				if (term == null || term.length() == 0)
					continue;
				termCounts.count(term);
			}

			// emit postings
			for (MapKI.Entry<String> e : termCounts.entrySet()) {
				word.set(e.getKey());
				
				double normalizedTF = 0;
				int rightElement = e.getValue();
				normalizedTF = (double)rightElement/(double)maxCount.intValue();
				
				output.collect(word, new PairOfInts((int) docno.get(), e.getValue(), normalizedTF));
			}
		}
	}

	//This reducer opens the map file and merges the key entries
	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, PairOfInts, Text, ArrayListWritable<PairOfInts>> {

		public void reduce(Text key, Iterator<PairOfInts> values,
				OutputCollector<Text, ArrayListWritable<PairOfInts>> output, Reporter reporter)
				throws IOException {
			ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

			while (values.hasNext()) {
				postings.add(values.next().clone());
			}
			output.collect(key, postings);
		}
	}

	private BuildInvertedIndex() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	//The fancy way of running the map reduce jobs.
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = 1;


		JobConf conf = new JobConf(BuildInvertedIndex.class);
		conf.setJobName("BuildInvertedIndex");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(PairOfInts.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		conf.setOutputFormat(MapFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		JobClient.runJob(conf);

		return 0;
	}

	//run our program.
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new BuildInvertedIndex(), args);
		System.exit(res);
	}

}
