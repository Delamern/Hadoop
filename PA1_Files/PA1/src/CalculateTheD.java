/*CIS 890
 * 
 * This class calculates the D values for all documents, and outputs them to a text file for
 * use in the query lookup.
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import cloud9.ArrayListWritable;
import cloud9.PairOfInts;





public class CalculateTheD {
	
	private static int numDocs; //Total number of documents in our collection
	private static double idf = 0; //The idf value for the given term
	
	private static String indexPath; //Stores the path where the index files are located 
	private static String collectionPath; //Stores the path for where the processed data files are
	private static Configuration config;
	private static FileSystem fs;
	private static MapFile.Reader reader;
	private static Text key;
	private static ArrayListWritable<PairOfInts> value;
	private static BufferedReader otherD;
	

	private static FSDataInputStream runThrough;
	
	private static double TheD = 0; //Stores the D value for the given document as we calcualte it.
	
	
	
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
	
	
	
	//This method does the magic of calculating D values.
	public static void Lookup(String line) throws IOException
	{
		//Reset D value so we don't keep adding to it forever.
		TheD = 0;
		
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
			idf = Math.log(numDocs/value.size())/Math.log(2);
			double tfidf = normalizedTF * idf;
			
			TheD += (tfidf * tfidf); //Add the ifidf^2 to TheD value.
			
			
		}
	}
	
	 
	//This method is from stack Overflow by Bohemian  
	//http://stackoverflow.com/questions/6790689/remove-duplicate-values-from-a-string-in-java
	public static String deDup(String s) {
	
		return new LinkedHashSet<String>(Arrays.asList(s.split(" "))).toString().replaceAll("(^\\[|\\]$)", "").replace(", ", " ");  
	}

	
	public static void main(String[] args) throws IOException {
		//If the person enters the wrong number of arguments, this is what gets printed.
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
			
			 //We're trying to create the DFile.txt here.
			 String filename = "DFile.txt";
			 BufferedWriter out = new BufferedWriter(new FileWriter(filename));
		      
		        
			 //Goes through every line in the file calculating D values
			 while((line = otherD.readLine())!=null)
			 {		
				 //System.out.println(line);	
				 Lookup(line); //Run the lookup query
					
				 //print TheD and docname as it calculates them	
				 String[] terms = line.split("\\s+"); //This is to get the filename for printing	
				 TheD = Math.sqrt(TheD);
				 String output = terms[1] + " " + TheD; //Combines the filename and D value onto one output line
				 System.out.println(output); //Prints the output so I know the program hasn't stalled out
				 out.write(output + "\n"); //Writes the output to the DFile.txt
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
