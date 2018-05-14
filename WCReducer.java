package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class WCReducer {

		private FileManager FM;
		private Common Cmn; 
		WCReducer(){
			this.FM = new FileManager();
			this.Cmn = new Common();
		}
		
		public void filterWord(String filterfile, String reducefile, String filted){
			BufferedReader reader = null;
			BufferedReader filter = null;
			List<Pair<String,Integer>> pairs = new ArrayList<Pair<String,Integer>>();
			String line = null;
			final String REGEX = ", ";
			try{
				PrintWriter writer= new PrintWriter(new FileWriter(filted, false));
				reader = new BufferedReader(new FileReader(reducefile));
				filter = new BufferedReader(new FileReader(filterfile));
				pairs = Cmn.MakePairs(reader, REGEX);
				int filter_count = 0;
				int reduce_count = pairs.size();
				while(( line = filter.readLine() ) != null) {
					
					for (Iterator<Pair<String, Integer>> iterator = pairs.iterator(); iterator.hasNext(); ) {
						Pair<String, Integer> pair = iterator.next();
						  if(pair.GetKey().equals(line)){
							  filter_count++;
						    iterator.remove();
						  }
					}
					
				}
				for(Pair<String,Integer> pair : pairs){
					FM.WriteRecord(pair.GetKey() + ", " + pair.GetValue(), writer);
				}
				System.out.println(System.getProperty("line.separator") + "@리듀스 수 "+  reduce_count + "필터 수 "  + filter_count +"");
				reader.close();
				writer.close();
				filter.close();
			
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
		
		public void select(String filted, int n){
			BufferedReader reader = null;
			String line= null;
			int wordcount = 0;
			int linecount = 0;
			try{
				reader = new BufferedReader(new FileReader(filted));
				while(( line = reader.readLine() ) != null) {
					linecount++;
					wordcount++;
					if(linecount == 10){
						linecount = 0;
						line += System.getProperty("line.separator");
					}
					System.out.print(" " + line);
					
					if(wordcount == n){
						break;
					}
				}
				reader.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		
		public void sortFreq(String textreduce, String reducefile) throws IOException {
			BufferedReader reader = null;
			List<Pair<String,Integer>> pairs = new ArrayList<Pair<String,Integer>>();
			final String REGEX = ", ";
			try{
				PrintWriter writer= new PrintWriter(new FileWriter(reducefile, false));
				reader = new BufferedReader(new FileReader(textreduce));
				pairs = Cmn.MakePairs(reader, REGEX);

				Collections.sort(pairs, Collections.reverseOrder(new SortbyValue()));
				
				String line = null;
				int linecount = 0;
				for(Pair<String,Integer> pair : pairs){
					line = pair.GetKey() + ", " + pair.GetValue();
					FM.WriteRecord(line, writer);
					
					System.out.print(line + " ");
					
					linecount++;
					if(linecount == 10){
						linecount = 0;
						System.out.print(System.getProperty("line.separator"));
					}
				}
				writer.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
		}
		
		
		public int reduceMerger(int numPart, String combinefile_path, String textreduce){
			
			BufferedReader reader = null;
			
			final String REGEX = ", ";
			Map<String, Integer> results = null;
			List<Pair<String,Integer>> total = new ArrayList<Pair<String,Integer>>();
			String combinefile = null;
			try{
				for(int i=0; i<numPart; i++){
					
					combinefile = combinefile_path + i + ".txt";
//					System.out.println("---------" +combinefile_i+"----------");
					reader = new BufferedReader(new FileReader(combinefile));

					total.addAll(Cmn.MakePairs(reader, REGEX));
					reader.close();
				}
				PrintLists(total.iterator());
				Cmn.Sort(total);
				results = Cmn.Combine(total);
				FM.WriteFiles(results, combinefile);

				int words = total.size();
				return words;
			}
			catch(Exception e){
				e.printStackTrace();
				final int WORDS_ERROR = -1;
				return WORDS_ERROR;
			}
			
			
		}
		

			public void PrintLists(Iterator iter){
				int linecount = 0;
				while(iter.hasNext()){
					Pair<String,Integer> tmp = (Pair<String, Integer>) iter.next();
					linecount++;
//					System.out.println("Key : " + tmp.GetKey() + " value : " + tmp.GetValue());
					System.out.print(tmp.GetKey() + ", " + tmp.GetValue() + " ");
					if(linecount == 10){
						linecount = 0;
						System.out.println("");
					}
					
				}
			}
}
