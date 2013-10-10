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
    
    //Now we are going to override the setup method so that it creates the hashtable
    @Override
    protected void setup(Mapper.Context context){

        Configuration conf = context.getConfiguration();
        
        BufferedReader br = null;
		try {
 
			String current;
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("stopwords.txt");
			
			br = new BufferedReader(new InputStreamReader(is)); //Open our list of stopwords
			//Reading each word from the stopwords text document, trimming them to get rid of whitespace, then adding
			//the word to the stopwords hashtable list.
			while ((current = br.readLine()) != null) {
				
				String trimmed = current.trim();
				stopWords.add(trimmed);
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
    
  
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	
        String line = value.toString();
        
        line = line.toLowerCase();
        line = line.replaceAll("\\<.*?\\>", "");
        line = line.replaceAll("-", " ");
        line = line.replaceAll("\\p{Punct}", "");
        
        //StringTokenizer tokenizer = new StringTokenizer(line);
        String[] terms = line.split("\\s+");
        PorterStemmer stemmer = new PorterStemmer();
        String outPut = "";
        for (String term : terms) {
            
        
            // this if is checking for badword conditions such as if the word is a number in disguise,
            //or if the word is secretly one of our stopwords. If it is, it breaks to go back to the start of the loop.
            if (term.equals("")) continue;
            if (Character.isDigit(term.charAt(0)) || stopWords.contains(term)) continue;
            
            
            term = stemmer.stem(term);
            outPut = outPut + " " + term;
            stemmer.reset();
        }
        
        //line = terms.toString();
        word = new Text(outPut);
        //If the previous if didn't detect any stop conditions, the word is processed and added to the count.
        context.write(word, null);
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
 
 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "ProcessData");
	    
	    job.setJarByClass(ProcessData.class);
	    job.setNumReduceTasks(0);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    //job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	    
	    System.out.println("The legend never diiiiieeeeesssss.");
	 }

 
}
