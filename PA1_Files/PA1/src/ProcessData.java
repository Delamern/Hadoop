/*CIS 890 PA1
 * Emily Jordan
 * This class does the processing work on the given data set to prepare the data for entry into the index.
 * It runs a single map job that processes the data, and sends out the processed data one document/line at 
 * a time to the map document part-m-00000
 * 
 * I use the lucene Porter Stemmer to stem the words
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.*;


        
public class ProcessData {
	
	 
	
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    
    //This is the declaration of the hashtable that contains the stopwords.
    private static Set<String> stopWords = new HashSet<String>();
    
    //Now we are going to override the setup method so that it creates the hashtable of stopwords.
    @Override
    protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context){

        @SuppressWarnings("unused")
		Configuration conf = context.getConfiguration();
        
        
        BufferedReader br = null; //declare the buffered reader
		try {
 
			String current;
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("stopwords.txt"); //set the InputStream to the stopwords text file.
			
			br = new BufferedReader(new InputStreamReader(is)); //Open our list of stopwords
			
			//Reading each word from the stopwords text document, trimming them to get rid of whitespace, then adding
			//the word to the stopwords hashsetlist.
			while ((current = br.readLine()) != null) {
				
				String trimmed = current.trim(); //Remove whitespace so we have only the word.
				stopWords.add(trimmed); //Add the word to the stopwords table.
			}
 
		} catch (IOException e) {
			
			e.printStackTrace();
			System.out.println("You done derped.");
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				System.out.println("You done derped 2.");
				ex.printStackTrace();
			}
		}
        
        

       
    }
    
  
    //Takes in a single document from the corpus and does the preprocessing work on it.
    //outputs the processed document into a map file.
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	
        String line = value.toString();
        
        //Converts all words in the file to lower case, removes all html tags, replaces - marks with spaces
        //then removes all other punctuation.
        line = line.toLowerCase();
        line = line.replaceAll("\\<.*?\\>", "");
        line = line.replaceAll("-", " ");
        line = line.replaceAll("\\p{Punct}", "");
        
        //Split the given line into a string array so that we can step through the words in the string
        String[] terms = line.split("\\s+");
        PorterStemmer stemmer = new PorterStemmer();
        String outPut = "";
        for (String term : terms) {
            
        
            // this if is checking for badword conditions such as if the word is a number in disguise,
            //or if the word is secretly one of our stopwords. If it is, it breaks to go back to the start of the loop.
            if (term.equals("")) continue;
            if (Character.isDigit(term.charAt(0)) || stopWords.contains(term)) continue;
            
            //This stems the given word, and adds it to the output string we are creating.
            term = stemmer.stem(term);
            outPut = outPut + " " + term;
            stemmer.reset();
        }
        
        //The processed output string is converted back into a Text, and then the mapper sends out the processed Text item
        //as its result, with null as its value.
        word = new Text(outPut);
        context.write(word, null);
    }
 } 
 
 /*
  * Ended up not using a reduce job for preprocessing the data, as I did not need the documents sorted.
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }*/
 
 
 //Calls the map job to run.
 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "ProcessData");
	    
	    job.setJarByClass(ProcessData.class);
	    job.setNumReduceTasks(0);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	   
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	    
	 }

 
}
