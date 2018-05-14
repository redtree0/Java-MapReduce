package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WCShuffler{
	
	private FileManager FM;
	private Common Cmn;
	
	WCShuffler(){
		this.FM = new FileManager();
		this.Cmn = new Common();
	}
	
	void sortCombiner(int i, String partfile_path, String combinefile_path){
		
		BufferedReader reader = null;
		List<Pair<String,Integer>> pairs = new ArrayList<Pair<String,Integer>>();
		String partfile = partfile_path + i + ".txt";
		String combinefile = combinefile_path + i + ".txt";	
		Map<String, Integer> results = null;
		final String REGEX = ", ";
		try{
			reader = new BufferedReader(new FileReader(partfile));
		
			pairs = Cmn.MakePairs(reader, REGEX);
			Cmn.Sort(pairs);
			results = Cmn.Combine(pairs);
			FM.WriteFiles(results, combinefile);
			reader.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	
}