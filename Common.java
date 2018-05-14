package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Common {
	
	public List<Pair<String,Integer>> MakePairs(BufferedReader reader, String regex) throws NumberFormatException, IOException{
		
		List<Pair<String,Integer>> results = new ArrayList<Pair<String,Integer>>();
		String line = null;
		
		while(( line = reader.readLine() ) != null) {
			String[] temp = line.split(regex);
			String key = temp[0];
			Integer value = Integer.parseInt(temp[1]);
			results.add(new Pair<String,Integer>(key, value));
		}
		
		return results;
	}
	
	public void Sort(List<Pair<String,Integer>> pairs){
		Collections.sort(pairs);
		Collections.sort(pairs,new Sortbyroll());
	}
	
	  public Map<String, Integer> Combine (List<Pair<String,Integer>> pairs){
		  
		  Map<String, Integer> results = new TreeMap<>();
		  
		  for(Pair<String, Integer> p : pairs){
				String key = p.GetKey();
				Integer sum = results.containsKey(key) ?  results.get(key) : 0;
				sum += p.GetValue();
				results.put(key, sum);
			}
		  
		  return results ;
	  }
	
}
