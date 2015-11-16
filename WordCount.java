/**
* @author Dustin Hurst
* CS 3250
* Assignemtn 3
* Parallelized Word Count that threads the word count into chunks and spits out the results into an output directory in the file results.txt
*/

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class WordCount implements Runnable {
	private final String buffer;
	private final ConcurrentMap<String,Integer> counts;

    	public WordCount(String buffer, ConcurrentMap<String,Integer> counts) {
        this.counts = counts;
        this.buffer = buffer;
    	}
	
	private final static String DELIMS = " :;,.{}()\t\n";

    	/**
	* findDelim
	* @param String buf
     	* Looks for the last delimiter in the string, and returns its
     	* index.
	* @return 0
     	*/
    	private static int findDelim(String buf) {
        	for (int i = buf.length() - 1; i>=0; i--) {
            		for (int j = 0; j < DELIMS.length(); j++) {
                		char d = DELIMS.charAt(j);
                		if (d == buf.charAt(i)) return i;
            		}
        	}
        	return 0;
    	}

    	/**
	* readFileAsString
	* @param BufferedReader reader, int size 
     	* Reads in a chunk of the file into a string.  
     	* @return fileData.toString();
	*/
    	private static String readFileAsString(BufferedReader reader, int size)throws java.io.IOException {
        	StringBuffer fileData = new StringBuffer(size);
        	int numRead=0;

        	while(size > 0) {
            		int bufsz = 1024 > size ? size : 1024;
            		char[] buf = new char[bufsz];
            		numRead = reader.read(buf,0,bufsz);
            		if (numRead == -1)
                	break;
            		String readData = String.valueOf(buf, 0, numRead);
            		fileData.append(readData);
            		size -= numRead;
        	}
        	return fileData.toString();
    	}	

    	/**
	* updateCount
	* @param String q
     	* Updates the count for each number of words. 
	*/
    	private void updateCount(String q) {
        	Integer oldVal, newVal;
        	Integer cnt = counts.get(q);
        	// first case: there was nothing in the table yet
        	if (cnt == null) {
            		oldVal = counts.put(q, 1);
            		if (oldVal == null)
			 return;
        	}
        	do {
            		oldVal = counts.get(q);
            		newVal = (oldVal == null) ? 1 : (oldVal + 1);
        	} while (!counts.replace(q, oldVal, newVal));
    	} 

    	/**
     	* Main task : tokenizes the given buffer and counts words. 
     	*/
    	public void run() {
        	StringTokenizer st = new StringTokenizer(buffer,DELIMS);
        	while (st.hasMoreTokens()) {
            		String token = st.nextToken();
            		//System.out.println("updating count for "+token);
            		updateCount(token);
        	}
    	} 
	
	/**
	* main
	* @param String args[]
	* executes the program
	*/
	public static void main(String args[]) throws java.io.IOException {
    		int chunkSize = 0;
		int numThreads = 0;
		File readFile = null;
		if (args.length < 3) { //check if right amount of args was entered.
       			System.out.println("Usage: java <file|directory> <chunk size 10-5000> <num of threads 1-100>");
           		System.exit(1);
        	}
		if(Integer.valueOf(args[1]) < 10 || Integer.valueOf(args[1]) > 5000){ //check if chunk size was entered in correctly
			System.out.println("Usage: java <file|directory> <chunk size 10-5000> <num of threads 1-100>");
			System.exit(0);
		}else
        		chunkSize = Integer.valueOf(args[1]);;
			if(Integer.valueOf(args[2]) < 1 || Integer.valueOf(args[2]) > 100){ //check if thread size was entered correctly
				System.out.println("Usage: java <file|directory> <chunk size 10-5000> <num of threads 1-100>");
				System.exit(0);
		}else
			numThreads = Integer.valueOf(args[2]);
			
		ExecutorService pool = Executors.newFixedThreadPool(numThreads);
		BufferedReader reader = null;
		try{
			reader = new BufferedReader(new FileReader(args[0]));
		}catch(IOException e){
			System.out.println("Usage: java <file|directory> <chunk size 10-5000> <num of threads 1-100>");
			System.exit(0);
		}
		ConcurrentMap<String,Integer> m = new ConcurrentHashMap<String,Integer>();
        	String leftover = ""; // in case a string broken in half
        	while (true) {
            		String res = readFileAsString(reader,chunkSize);
            		if (res.equals("")) {
                		if (!leftover.equals("")) 
                    			new WordCount(leftover,m).run();
                	break;
            		}
            		int idx = findDelim(res);
           	 	String taskstr = leftover + res.substring(0,idx);
            		leftover = res.substring(idx,res.length());
            		pool.submit(new WordCount(taskstr,m));
        	}
        	pool.shutdown();
        	try {
            		pool.awaitTermination(1,TimeUnit.DAYS);
        	} catch (InterruptedException e) {
            		System.out.println("Pool interrupted!");
            		System.exit(1);
        	}

		File dir; //output directory
    		File file; //results.txt file
		BufferedWriter out;

    		// create your filewriter and bufferedreader
    		dir = new File("./output");
    		file = new File("./output/results.txt");
		out = new BufferedWriter(new FileWriter(file));
		String formatStr = "%-15s %d\n";
        	int total = 0;
		MyComparator comparator = new MyComparator(m);
		Map<String,Integer> e = new TreeMap<String,Integer>(comparator);
		e.putAll(m);
		
		// printing loop
		for (Map.Entry<String,Integer> entry : e.entrySet()) {
        		int count = entry.getValue();
                	//System.out.format(formatStr,entry.getKey(),count);
			out.write(String.format(formatStr,entry.getKey(),count));
	    		total += count;
        	}
		String wordTotal = "Total words = " + total;
		//System.out.println(wordTotal);
		out.write(wordTotal);
		out.close();
    	}

}
