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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.PorterStemmer;

import cloud9.ArrayListWritable;
import cloud9.PairOfInts;

public class LookupQuery {
	
	
	//private static int totalNumberOfTimesTheWordOccursEverywhere; //Total number of times the word occurs across all documents
	private static int numDocs; //Total number of documents in our collection
	private static double Qvalue; //Storing the |Q| of the query
	
	private static boolean areThereMoreLookups; //Keeps track of if user wants to do more queries
	private static String query; //The string holding the users query
	private static String dups; 
	
	private static String indexPath;
	private static String collectionPath;
	private static Configuration config;
	private static FileSystem fs;
	private static MapFile.Reader reader;
	private static Text key;
	private static ArrayListWritable<PairOfInts> value;
	private static BufferedReader d;

	private static FSDataInputStream collection;
	
	//Hashtable creation learned from this article on stack overflow
	//http://stackoverflow.com/questions/29324/how-do-i-create-a-hash-table-in-java
	private static final Hashtable<String,Double> dValues = new Hashtable<String,Double>();
	
	private static Hashtable<String,Double> queryTF = new Hashtable<String,Double>();
	
	//stopwords hastable
	 private static Set<String> stopWords = new HashSet<String>();
	 
	 private static Hashtable<String,Double> docTFIDF = new Hashtable<String,Double>();
	
	//Sets up all our path variables and readers.
	public static void initQuery(String[] args) throws IOException{
		indexPath = args[0];
		collectionPath = args[1];

		config = new Configuration();
		fs = FileSystem.get(config);
		reader = new MapFile.Reader(fs, indexPath, config);
		
		key = new Text();
		value = new ArrayListWritable<PairOfInts>();
		areThereMoreLookups = true;
		query = "";
		Qvalue = 0;
		

	}
	
	//This method is written assuming that the file DFile.txt has already been created.
	//based off this article on stack overflow
	//http://stackoverflow.com/questions/13904816/assigning-numbers-from-text-file-to-hashtable
	//and this input stream tutorial  http://www.tutorialspoint.com/java/io/inputstream_read.htm
	public static void createDHash() throws IOException
	{
		String current;
		InputStream is = new FileInputStream("DFile.txt");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is)); //Open our list of D Values
		//Reading each word from the DFile text document, then adding them to the dValues hashtable
		while ((current = br.readLine()) != null) {
			String[] broken = current.split(" ");
			dValues.put(broken[0], Double.parseDouble(broken[1]));

		}

		br.close();	
	}
	
	
	//This is the method that creates the stopwords list
	public static void createStopwords() throws IOException{
		
		
		String current;
		URL f = LookupQuery.class.getClassLoader().getResource("stopwords.txt");
		BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			
		while ((current = br.readLine()) != null) {
				
			String trimmed = current.trim();
			stopWords.add(trimmed);
		}

		br.close();
	}
	

	
	public static void wordLookup(String word) throws IOException
	{
			
		key.set(word);
		reader.get(key, value);
		
		Writable w = reader.get(key, value);
		if (w == null){
			return;
		}
		
		
		double idf = Math.log(numDocs/value.size())/Math.log(2);
		double querytf = queryTF.get(word);
		double qfidf = querytf * idf;
		
		Qvalue += (qfidf * qfidf);
		
		for (PairOfInts pair : value) {

			//Open the collection and buffered reader.
			collection = fs.open(new Path(collectionPath));
			d = new BufferedReader(new InputStreamReader(collection)); 
			

			//This line seeks out the location of the document in the file with the given offset
			collection.seek(pair.getLeftElement());
			String s = d.readLine();
			String[] terms = s.split("\\s+");
			String filename = terms[1];
			
			double normalizedTF = pair.getnormTF();
			
			idf = Math.log(numDocs/value.size())/Math.log(2);
			double tfidf = normalizedTF * idf;
			
			double TfidfQtfIdf = tfidf * (querytf * idf);
			
			
					
			
			if(!docTFIDF.containsKey(filename)) docTFIDF.put(filename, TfidfQtfIdf);
			else {
				double temp = docTFIDF.get(filename);
				temp += TfidfQtfIdf;
				docTFIDF.remove(filename);
				docTFIDF.put(filename, temp);
			}
			
		
			collection.close();
			d.close();
			
		}//End for loop of PairOfInts in value
		
		 
	}
	
	//Run this to get the total number of documents in the collection
	public static void CountDocs() throws IOException
	{
		collection = fs.open(new Path(collectionPath));
		d = new BufferedReader(new InputStreamReader(collection)); 
		
		numDocs = 0;
		while(d.readLine() != null) numDocs++;
		
		d.close();
		collection.close();
	}
	
	
	//This method calculates the max term frequency of a given document.
	public static int maxFreqCalculator(String[] terms)
	{
		//This word counting method is from 
		//http://stackoverflow.com/questions/14594102/java-most-suitable-data-structure-for-finding-the-most-frequent-element
		//written by maerics Jan 23, 2013
		//It is figuring out the max term frequency of a given document the hard way.
		Integer maxCount = 0;
		Map<String, Integer> wordCount = new HashMap<String, Integer>();
		for (String str : terms) {
		  if (!wordCount.containsKey(str)) { wordCount.put(str, 0); }
		  int count = wordCount.get(str) + 1;
		  if (count > maxCount) {
		    maxCount = count;
		  }
		  wordCount.put(str, count);
		}
		return maxCount.intValue();
		
	}
	
	public static String deDup(String s) {
		
		return new LinkedHashSet<String>(Arrays.asList(s.split(" "))).toString().replaceAll("(^\\[|\\]$)", "").replace(", ", " ");
	}
	
	public static String procQuery(String line){
			
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
	            
	            //Stem the term.
	            term = stemmer.stem(term);
	            outPut = outPut + " " + term;
	            stemmer.reset();
	        }
	        //string with duplicate words still in it
	        dups = outPut;
	        outPut = deDup(outPut);
	        
	        return outPut;
	}
	
	public static void countFreqWord()
	{
		
		int count = 0;
		String[] terms2 = query.split(" ");//split the query into a string[]
		String[] terms = dups.split(" "); //split the query into a string[]
		int maxFreq = maxFreqCalculator(terms); //get the max
		
		for(String term: terms2){
			
			count = 0;
			for(String t: terms){
				if(term.equals(t)) count++;
			}
			double normalizedTF = (double)count / (double)maxFreq;
			queryTF.put(term, normalizedTF);
			
		}

	}
	
	
	public static void Lookup() throws IOException
	{
		//Make certain the word is not empty
		if(query.equals("")){
			System.out.println("Invalid word.");
			return;
		}
		
		query = procQuery(query);
		if(query.equals("")){ System.out.println("No results"); return;}
		
		countFreqWord();
		
		String[] terms = query.split(" ");
		Qvalue = 0;
		
		//calls wordLookup for each word in the query
		//wordLookup calculates the summation of the tf*idf * (qtf * idf) for each document
		//for each word in the query and stores it in docTFIDF, where key is filename, value is tfidfqtfidf
		for(String term: terms) wordLookup(term);
		
		Qvalue = Math.sqrt(Qvalue);
		
		//Now we need to do the cosine similarity fun times
		//take the value from docTFIDF / (|D| * |Q|)
		//Using an enumerator to step through, as per this article on stack overflow:
		//http://stackoverflow.com/questions/2351331/iterating-hashtable-in-java
		Enumeration<String> enumKey = docTFIDF.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
		    Double val = docTFIDF.get(key);
		    
		    double newVal = val / (dValues.get(key) * Qvalue);
		    docTFIDF.put(key, newVal);
		}
		
		//Now to sort the values in the hashtable into their final form
		sortValue(docTFIDF);
	
		
	}
	
	//This method is largely from http://stackoverflow.com/questions/5176771/sort-hashtable-by-values
	//It takes the Hashtable of documents and their values, and sorts them by their value so we 
	//can print the top 10 results.
	public static void sortValue(Hashtable<String, Double> t){
	      
		//Transfer as List and sort it
		ArrayList<Map.Entry<String, Double>> l = new ArrayList<Entry<String, Double>>(t.entrySet());       
		Collections.sort(l, new Comparator<Map.Entry<String, Double>>(){
			public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}});
	       
		for(int i = 0; i < 10; i++)
		{
			if(l.size() > i){
				System.out.println(l.get(i).toString());
			}
		}    
	}
			
	

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		initQuery(args);
		
		CountDocs(); //Figure out how many documents are in the collection
		createDHash();
		createStopwords();
		
		Scanner in = new Scanner(System.in); //create the scanner to read the user input from console
		
		//Loop that keeps asking user for words to lookup
		while(areThereMoreLookups)
		{
			
			System.out.println("Query to lookup: ");
			query = in.nextLine(); //get the word from the input line
			Lookup(); //Run the lookup query
			
			System.out.println("Do you want to check another word? yes/no ");
			String yesNo = in.nextLine();
			if(!yesNo.equals("yes")) areThereMoreLookups = false;		
			
		}


		//collection.close();
		reader.close();
	}
}
