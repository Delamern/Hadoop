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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;





public class LookupPostings {
	
	
	//private static int totalNumberOfTimesTheWordOccursEverywhere; //Total number of times the word occurs across all documents
	private static int numberOfDocsWordOccursIn; //Total number of documents that contain the word
	private static int numDocs; //Total number of documents in our collection
	private static int maxFrequencyThisDoc; //Max term frequency of a given document
	//private static int wordsInThisDocument; //How many words are in the document we are processing
	private static List<Listing> myList; //List holding document information for printing
	//private static double idf; //The idf value for the given term
	
	
	private static boolean areThereMoreLookups;
	private static String wordToFind;
	private static String unstemmed;
	
	private static String indexPath;
	private static String collectionPath;
	private static Configuration config;
	private static FileSystem fs;
	private static MapFile.Reader reader;
	private static Text key;
	private static ArrayListWritable<PairOfInts> value;
	private static BufferedReader d;
	private static PorterStemmer ps;

	private static FSDataInputStream collection;
	
	//Hashtable creation learned from this article on stack overflow
	//http://stackoverflow.com/questions/29324/how-do-i-create-a-hash-table-in-java
	private static final Hashtable<String,Double> dValues = new Hashtable<String,Double>();
	
	//Sets up all our path variables and readers.
	public LookupPostings(String[] args) throws IOException{
		indexPath = args[0];
		collectionPath = args[1];

		config = new Configuration();
		fs = FileSystem.get(config);
		reader = new MapFile.Reader(fs, indexPath, config);
		
		key = new Text();
		value = new ArrayListWritable<PairOfInts>();
		areThereMoreLookups = true;
		wordToFind = "";
		unstemmed = "";
		ps = new PorterStemmer();
		//idf = 0;
		
		//This is a list for holding the document name and number of times the word occured for printing.
		myList = new ArrayList<Listing>();

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
	
	//Resets all the variables between word lookups.
	public static void LookupReset()
	{
		//Initializing a few variables. More specifically, the total number of occurances of the word over all documents
		//and the total number of documents the word occurs in holders.
		//totalNumberOfTimesTheWordOccursEverywhere = 0;
		numberOfDocsWordOccursIn = 0;
		maxFrequencyThisDoc = 0;
		//wordsInThisDocument = 0;
		//idf = 0;
		myList.clear();
		
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
	
	
	public static void Lookup() throws IOException
	{
		//Make certain the word is not empty
		if(wordToFind.equals("")){
			System.out.println("Invalid word.");
			return;
		}
		
		//Call the reset method to reset all variable to their base before we begin.
		LookupReset();
		
		unstemmed = wordToFind; //save an unstemmed version of the word for printing
		wordToFind =  wordToFind.replaceAll("\\p{Punct}", ""); //parse out punctuation
		wordToFind = ps.stem(wordToFind); //stem the word we are looking for to match collection format
        ps.reset(); //reset the stemmer so it is ready for the next word.
		
		System.out.println("Looking up postings for the stemmed term " + wordToFind);
		key.set(wordToFind);
		
		reader.get(key, value);
		
		//Check to make certain there is an entry to work with. If there isn't, displays message and leaves method.
		Writable w = reader.get(key, value);
		if (w == null){
			System.out.println("The term did not exist in this collection.");
			return;
		}
		
		for (PairOfInts pair : value) {
			
			//Open the collection and buffered reader.
			collection = fs.open(new Path(collectionPath));
			d = new BufferedReader(new InputStreamReader(collection)); 
			
			//printing the offset and total number of times the word occurs in that document
			System.out.print(pair);
			//This line seeks out the location of the document in the file with the given offset
			collection.seek(pair.getLeftElement());
			String s = d.readLine();
			String[] terms = s.split("\\s+");
			
			
			maxFreqCalculator(terms);
						
			//Acquires the number of times a word appears in the current document
			int timesTheWordOccuredInThisDoc = pair.getRightElement();
			
			//This one figures out how many words are in a given document
			//wordsInThisDocument = terms.length - 2;
			
			//this is adding to the running total of number of times the word has occured across all documents
			//totalNumberOfTimesTheWordOccursEverywhere += timesTheWordOccuredInThisDoc;
			
			//This keeps track of how many documents the word occurs in.
			numberOfDocsWordOccursIn ++;

			//closes the reader so that the bufferedreader will return to the beginning of
			//the file for the next seek it does.
			d.close();
			collection.close();
			
			//Calculates the noramalized value of the TF.
			double normalizedTF = (double)timesTheWordOccuredInThisDoc/(double)maxFrequencyThisDoc;
			
			//Store filename and number of times the word occured in that document and docs tf
			System.out.println(terms[1]);
			double d = dValues.get(terms[1]);
			Listing temp = new Listing();
			temp.docName = terms[1];
			temp.count = timesTheWordOccuredInThisDoc;
			temp.tf = normalizedTF;
			temp.d = d;
			myList.add(temp);
			
		}//End for loop
		

		//Calculates the idf of the given word.
		//idf = Math.log(numDocs/numberOfDocsWordOccursIn)/Math.log(2);
		
		printLookup();
		
	}
	
	//This is the method that prints everything about the word we just 
	//did a search for.
	public static void printLookup()
	{
		System.out.print(unstemmed + ":" + numberOfDocsWordOccursIn + ": ");
		//for(Listing l: myList) System.out.print("(" + l.docName + "," + l.count + ") ");
		
		//System.out.print("\n" + unstemmed + "(IDF=" +idf + ") occurs in: \n" );
		
		for(Listing l: myList) System.out.println(l.docName + " " + l.count + "times; Possible D=" +  l.d);
	 
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		
		LookupPostings lp  = new LookupPostings(args);
		CountDocs(); //Figure out how many documents are in the collection
		createDHash();
		
		Scanner in = new Scanner(System.in); //create the scanner to read the user input from console
		
		//Loop that keeps asking user for words to lookup
		while(areThereMoreLookups)
		{
			
			System.out.println("Word to lookup: ");
			wordToFind = in.next(); //get the word from the input line
			Lookup(); //Run the lookup query
			
			System.out.println("Do you want to check another word? yes/no ");
			String yesNo = in.next();
			if(!yesNo.equals("yes")) areThereMoreLookups = false;		
			
		}


		//collection.close();
		reader.close();
	}
}
