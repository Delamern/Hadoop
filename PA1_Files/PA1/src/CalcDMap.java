/*CIS 890
 * 
 * This class calculates the D values for all documents, and outputs them to a text file for
 * use in the query lookup.
 */


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;



import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;







import cloud9.ArrayListWritable;
import cloud9.PairOfInts;





public class CalcDMap {
	
	

	

	
	//Run this to get the total number of documents in the collection
	public static int CountDocs(String args) throws IOException
	{
		int numDocs = 0;
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		FSDataInputStream collection = fs.open(new Path(args));
		BufferedReader d = new BufferedReader(new InputStreamReader(collection)); 
	
		while(d.readLine() != null) numDocs++;
		
		d.close();
		collection.close();
		
		return numDocs;
	}
	
	//This method does the magic of calculating D values.
		
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private Text word = new Text();
	    @SuppressWarnings("unused")
		private String index;
	    private int numDocs;
	    @SuppressWarnings("unused")
		private String num;
	    
	    public void configure(JobConf job) {
	        index = job.get("index");
	        num = job.get("numDocs");
	    }
	  
	    //Takes in a single document from the corpus and does the preprocessing work on it.
	    //outputs the processed document into a map file.
	    public void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {	    	
	        String line = value1.toString();
	        
	        String[] terms = line.split(" ");

				
	        Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			//String inPath = index;
			String inPath = "/user/delamern/cran-index/part-00000";
			//numDocs = Integer.parseInt(num);
			numDocs = 1400;
			MapFile.Reader reader = new MapFile.Reader(fs, inPath, config);
			Text key = new Text();
			ArrayListWritable<PairOfInts> value = new ArrayListWritable<PairOfInts>();
	        
	        
	        
	        double dValue = Lookup(line, key, value, reader, numDocs);
	        
	        dValue = Math.sqrt(dValue);
	     
	        String output = terms[1] + " " + dValue;
	        
	        
	        //The processed output string is converted back into a Text, and then the mapper sends out the processed Text item
	        //as its result, with null as its value.
	        word = new Text(output);
	        context.write(word, null);
	        reader.close();
	    }
	    
	    
	    
	    public static double Lookup(String line, Text key, ArrayListWritable<PairOfInts> value, MapFile.Reader reader, int numDocs) throws IOException
		{
	    	//Reset D value so we don't keep adding to it forever.
			double TheD = 0;
			
			//Remove duplicate words so that we won't be doing multiple lookups for the same word
			String noDup = deDup(line);
			String[] splitArray = noDup.split(" ");
			
			List<String> noDupTerms = new ArrayList<String>(Arrays.asList(splitArray));
					
			//Testing to improve performance so this doesn't take all day
			long testing = 0; //The offset of the document we are looking at.
			
			
			//Set up the testing comparator
			key.set(noDupTerms.get(1));
			noDupTerms.remove(1);
			noDupTerms.remove(0);
			reader.get(key, value);
			//Check to make certain there is an entry to work with. 
			Writable w = reader.get(key, value);
			if (w != null){
				for(PairOfInts pair : value) testing = pair.getLeftElement();
			}
				
			
			
			//Stepping through all the terms in our file.
			for(String term : noDupTerms)
			{
			
				key.set(term);
				reader.get(key, value);
				
				//Check to make certain there is an entry to work with. 
				w = reader.get(key, value);
				if (w == null){
					continue;
				}
				
				double normalizedTF = 0;
			
				for (PairOfInts pair : value) {
					//This gets us the pair we need to work with.
					if(testing == pair.getLeftElement()){
						normalizedTF = pair.getnormTF();
						break;
					}
				}//End for loop of PairOfInts in value
				
				
				//Calculates the idf of the given word, then the tfidf, then adds that number squared to the total D for that document.
				double idf = Math.log(numDocs/value.size())/Math.log(2);
				double tfidf = normalizedTF * idf;
				
				TheD += (tfidf * tfidf); //Add the ifidf^2 to TheD value.
				
			
				
				
			}//end for loop noDupTerms
			
			return TheD;
		}
		
	    
	    
	    
	    
	 } 
	
	

	

	

	

		//This method is from stack Overflow by Bohemian  
		//http://stackoverflow.com/questions/6790689/remove-duplicate-values-from-a-string-in-java
		public static String deDup(String s) {
		
			return new LinkedHashSet<String>(Arrays.asList(s.split(" "))).toString().replaceAll("(^\\[|\\]$)", "").replace(", ", " ");  
		}

	
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("usage: [index-path] [collection-path] [output-path]");
			System.exit(-1);
		}
		
		String index = args[0];
		String collection = args[1];

	
		Configuration config = new Configuration();
				
		int numDocs1 = CountDocs(collection); //Figure out how many documents are in the collection
		String no = Integer.toString(numDocs1);

		config.set("index", index);
		config.set("numDocs", no);
		
		Job job = new Job(config, "CalcDMap");
	    
	    job.setJarByClass(CalcDMap.class);
	    job.setNumReduceTasks(0);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	        
	    job.setMapperClass(Map.class);
	   
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	        
	    job.waitForCompletion(true);
		
		
		        
	
	}
}