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


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;





public class CalculateTheD {
	
	private static int numberOfDocsWordOccursIn; //Total number of documents that contain the word
	private static int numDocs; //Total number of documents in our collection
	private static int maxFrequencyThisDoc; //Max term frequency of a given document
	private static double idf = 0; //The idf value for the given term
	
	private static String indexPath;
	private static String collectionPath;
	private static Configuration config;
	private static FileSystem fs;
	private static MapFile.Reader reader;
	private static Text key;
	private static ArrayListWritable<PairOfInts> value;
	private static BufferedReader otherD;
	

	private static FSDataInputStream runThrough;
	
	private static double TheD = 0;
	
	
	

	
	//Resets all the variables between word lookups.
	public static void LookupReset()
	{
		//Initializing a few variables. More specifically, the count of how many documents have the word
		//the max frequency variable, and how many words are in the document, and the idf.
		numberOfDocsWordOccursIn = 0;
		maxFrequencyThisDoc = 0;
		idf = 0;
		TheD = 0;
	}
	
	//Run this to get the total number of documents in the collection
	public static void CountDocs() throws IOException
	{
		FSDataInputStream collection = fs.open(new Path(collectionPath));
		BufferedReader d = new BufferedReader(new InputStreamReader(collection)); 
		
		numDocs = 0;
		while(d.readLine() != null) numDocs++;
		
		d.close();
		collection.close();
	}
	
	
	//This method calculates the max term frequency of a given document.
	public static void maxFreqCalculator(String[] terms)
	{
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
		maxFrequencyThisDoc = maxCount.intValue();
		
	}
	
	
	public static void Lookup(String line) throws IOException
	{
		

		//Call the reset method to reset all variable to their base before we begin.
		LookupReset();
		
		//Find the highest frequency in the given document
		String[] terms = line.split("\\s+");
		maxFreqCalculator(terms);
		
		//Remove duplicate words so that we won't be doing multiple lookups for the same word
		String noDup = deDup(line);
		String[] noDupTerms = noDup.split("\\s+");
				
		//Testing to improve performance so this doesn't take all day
		long testing = 0;
		String curFileName = noDupTerms[1];
		
		//System.out.println(noDupTerms.length);
		for(String term : noDupTerms)
		{
			//System.out.println("term " + term);
			if(noDupTerms.length > 1 && term.equals(curFileName)){
				key.set(term);
				reader.get(key, value);
				for(PairOfInts pair : value) testing = pair.getLeftElement();
				continue;
			}			
			
			
			key.set(term);
			reader.get(key, value);
			//Check to make certain there is an entry to work with. If there isn't, displays message and leaves method.
			Writable w = reader.get(key, value);
			if (w == null){
				continue;
			}
			
			double normalizedTF = 0;
			numberOfDocsWordOccursIn = 0;
			idf = 0;

			for (PairOfInts pair : value) {
				
				//This keeps track of how many documents the word occurs in.
				numberOfDocsWordOccursIn ++;
				
				//Checks to see if the current pair is the document we are working with, if yes does calculations
				if(testing == pair.getLeftElement()){
					//Acquires the number of times a word appears in the current document
					int timesTheWordOccuredInThisDoc = pair.getRightElement();
					normalizedTF = (double)timesTheWordOccuredInThisDoc/(double)maxFrequencyThisDoc;
				}
				
			}//End for loop of PairOfInts in value
			
			//Calculates the idf of the given word, then the tfidf, then adds that number squared to the total D for that document.
			idf = Math.log(numDocs/numberOfDocsWordOccursIn)/Math.log(2);
			double tfidf = normalizedTF * idf;
			
			TheD += (tfidf * tfidf);
			
			
		}
        

		
	}
	
	 
	//This method is from stack Overflow by Bohemian  
	//http://stackoverflow.com/questions/6790689/remove-duplicate-values-from-a-string-in-java
	public static String deDup(String s) {
	
		return new LinkedHashSet<String>(Arrays.asList(s.split(" "))).toString().replaceAll("(^\\[|\\]$)", "").replace(", ", " ");
	    }

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		//Set up the initial values of the variables
		indexPath = args[0];
		collectionPath = args[1];
		
		config = new Configuration();
		fs = FileSystem.get(config);
		reader = new MapFile.Reader(fs, indexPath, config);
		key = new Text();
		value = new ArrayListWritable<PairOfInts>();

		
		
		CountDocs(); //Figure out how many documents are in the collection
		
		runThrough = fs.open(new Path(collectionPath));
		otherD = new BufferedReader(new InputStreamReader(runThrough)); 
		
		
		String line = "";
		 try {
			 String filename = "DFile.txt";
			 //String fullPath = collectionPath + "\\" + filename;
			 
			 //File file = new File(collectionPath, filename);
		        BufferedWriter out = new BufferedWriter(new FileWriter(filename));
		      //Loop that keeps asking user for words to lookup
				while((line = otherD.readLine())!=null)
				{
					//System.out.println(line);
					Lookup(line); //Run the lookup query
					// System.out.println("Is This Silently Failing? 5");
					//print TheD and docname
					String[] terms = line.split("\\s+");
					TheD = Math.sqrt(TheD);
					String output = terms[1] + " " + TheD;
					System.out.println(output);
					out.write(output + "\n");
					out.flush();
					
					
				}
		            
				
		        out.close();
		        
		 } catch (IOException e) {}
		

		otherD.close();
		runThrough.close();

		//collection.close();
		reader.close();
	}
}
