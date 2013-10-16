/*CIS 890 PA1
 * Emily Jordan
 * This class is based off the lookup class from Cloud9 for Hadoop
 * Code has been modified, and new methods and math added
 * to handle the tfidf calculations as well as the print
 * storage that was needed for this project.
 * 
 */


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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

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





public class LookupPostings {
	
	private static int numDocs; //Total number of documents in our collection
	private static List<Listing> myList; //List holding document information for printing
	
	
	private static boolean areThereMoreLookups; //boolean that keeps track of if the user wants to keep looking for more words
	private static String unstemmed; //The unstemmed saved version of the word for making the print pretty
	private static int numberOfDocsWordOccursIn;
	
	private static String indexPath; //Path to the index of words
	private static String collectionPath; //Path to the original processed document
	private static Configuration config;
	
	private static FileSystem fs;
	private static MapFile.Reader reader;
	private static Text key;
	private static ArrayListWritable<PairOfInts> value;
	private static BufferedReader d;
	private static PorterStemmer ps; //The porter stemmer. Written by Lucene.

	private static FSDataInputStream collection;
	
	//Hashtable creation learned from this article on stack overflow
	//http://stackoverflow.com/questions/29324/how-do-i-create-a-hash-table-in-java
	private static final Hashtable<String,Double> dValues = new Hashtable<String,Double>();
	
	//Sets up all our path variables and readers.
	public static void initVariables(String[] args) throws IOException{
		indexPath = args[0]; //Sets the index path to the first command line argument
		collectionPath = args[1]; //Sets the collection path to be the second command line argument

		config = new Configuration();
		fs = FileSystem.get(config);
		reader = new MapFile.Reader(fs, indexPath, config);
		
		key = new Text();
		value = new ArrayListWritable<PairOfInts>();
		areThereMoreLookups = true;
		unstemmed = "";
		ps = new PorterStemmer();
		
		
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
		InputStream is = new FileInputStream("DFile.txt"); //Set our input stream to read the DFile.txt
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is)); //Open our list of D Values
		//Reading each word from the DFile text document, then adding them to the dValues hashtable
		while ((current = br.readLine()) != null) {
			String[] broken = current.split(" ");
			dValues.put(broken[0], Double.parseDouble(broken[1]));

		}
		br.close();	
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
	
	
	
	//Method that does all the work for looking up the given word.
	public static void Lookup(String wordToFind) throws IOException
	{
		//Make certain the word is not empty
		if(wordToFind.equals("")){
			System.out.println("Invalid word.");
			return;
		}
		
		//Reset the list holding our results.
		myList.clear();
		
		//This block takes the word and changes it so that it matches the format of the processed file
		unstemmed = wordToFind; //save an unstemmed version of the word for printing
		wordToFind =  wordToFind.replaceAll("\\p{Punct}", ""); //parse out punctuation
		wordToFind = ps.stem(wordToFind); //stem the word we are looking for to match collection format
        ps.reset(); //reset the stemmer so it is ready for the next word.
		
        //Print statement so we can take a look at the word once we got done stemming and fixing it up.
		System.out.println("Looking up postings for the stemmed term " + wordToFind);
		
		
		key.set(wordToFind);
		reader.get(key, value);
		numberOfDocsWordOccursIn = value.size();
		
		//Check to make certain there is an entry to work with. If there isn't, displays message and leaves method.
		Writable w = reader.get(key, value);
		if (w == null){
			System.out.println("The term did not exist in this collection.");
			return;
		}
		
		//Now to step through all the documents that contain the given word.
		for (PairOfInts pair : value) {
			
			//Open the collection and buffered reader so we can look at the document that contains the word
			collection = fs.open(new Path(collectionPath));
			d = new BufferedReader(new InputStreamReader(collection)); 
			
			//This line seeks out the location of the document in the file with the given offset
			collection.seek(pair.getLeftElement());
			String s = d.readLine();
			String[] terms = s.split("\\s+");
			
						
			//Acquires the number of times a word appears in the current document
			int timesTheWordOccuredInThisDoc = pair.getRightElement();


			//closes the reader so that the bufferedreader will return to the beginning of
			//the file for the next seek it does.
			d.close();
			collection.close();
			
			//Calculates the noramalized value of the TF.
			double normalizedTF = pair.getnormTF();
			
			//Store filename and number of times the word occured in that document and docs tf
			double d = dValues.get(terms[1]);
			double idf = Math.log(numDocs/value.size())/Math.log(2);
			double tfidf = normalizedTF * idf;
			
			Listing temp = new Listing();
			temp.docName = terms[1];
			temp.count = timesTheWordOccuredInThisDoc;
			temp.tf = normalizedTF;
			temp.d = d;
			temp.idf = idf;
			temp.tfidf = tfidf;
			myList.add(temp);
			
		}//End for loop
		
		
		printLookup();
		
	}
	
	//This is the method that prints everything about the word we just 
	//did a search for.
	public static void printLookup()
	{
		System.out.println(unstemmed + ":" + numberOfDocsWordOccursIn + " IDF: " + myList.get(0).idf);

		
		for(Listing l: myList) System.out.println(l.docName + " " + l.count + "times; Possible D=" +  l.d);
	 
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [index-path] [collection-path]");
			System.exit(-1);
		}

		
		initVariables(args);
		CountDocs(); //Figure out how many documents are in the collection
		createDHash();
		
		Scanner in = new Scanner(System.in); //create the scanner to read the user input from console
		
		//Loop that keeps asking user for words to lookup
		while(areThereMoreLookups)
		{
			
			System.out.println("Word to lookup: ");
			String wordToFind = in.next(); //get the word from the input line
			Lookup(wordToFind); //Run the lookup query
			
			System.out.println("Do you want to check another word? yes/no ");
			String yesNo = in.next();
			if(!yesNo.equals("yes")) areThereMoreLookups = false;		
			
		}


		//collection.close();
		reader.close();
	}
}
