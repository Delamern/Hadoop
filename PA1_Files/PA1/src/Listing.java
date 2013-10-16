//Data struct for storing information for printing.
public class Listing {
    public String docName;
    public int count;
    double tf;
    double d;
    double idf;
    double tfidf;
    public Listing()
    {
    	docName = "";
    	count = 0;
    	tf = 0;
    	idf = 0;
    	tfidf = 0;
    }
}
