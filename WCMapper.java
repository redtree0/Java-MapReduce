package mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class WCMapper {
	
	private FileManager FM;
	private boolean Once = true;
	
	WCMapper(){
		this.FM = new FileManager();
	}
	
	public void mapper(int block_index, String splitfile_path, int numPart, String partfile_path){
		String splitfile = splitfile_path + block_index + ".txt";
		String partfile = null;
		
		RemovePartFiles(partfile_path, numPart);
	
		SplitFiletoWords(splitfile, numPart ,partfile_path);
	}

	public void RemovePartFiles(String partfile_path, int numPart){
		String partfile = null;
		if(Once){
			for(int i=0; i<numPart; i++){
				partfile = partfile_path +i+".txt";
				FM.RemoveFiles(partfile);
			}
			Once = false;
		}
	}
	
	  public void SplitFiletoWords(String splitfile, int numPart ,String partfile_path){
		  BufferedReader reader = null; 
		  
		  try {    
				reader = new BufferedReader(new FileReader(splitfile));
				
				
				String line = null; // 입력 라인의 전처리 용 버퍼
				
				int partition_index = -1;
				
				List<PrintWriter> partitions = FM.CreatePrintWriters(partfile_path, numPart, true);
				while(( line = reader.readLine() ) != null) { // 파일의 각 레코드

					  line = line.replaceAll("[\\n\\t?!\\,.\")(; ]", " ");
		              line = line.replace("-"," "); 
		              line =line.replaceAll("'+"," ");
		              line =line.replaceAll("\\s+"," ");
		               
					  String temp[] = line.split(" "); // split으로 토큰을 분할
	  
		              for(String s : temp) {
		            	  
		            	  if(!s.equals("")){
		            		  
		            		  partition_index = GetPartitionIndex(s.hashCode(), numPart);
				        	  
				        	  FM.WriteRecord(s + ", 1", partitions.get(partition_index));
		            	  }
			        	  
			          }

				} 	
				
				reader.close();
				
				FM.ClosePrintWriters(partitions);
				
		} catch (Exception e) {
		    e.printStackTrace();
		}  
	  }
	  
	  public int GetPartitionIndex(int hashcode, int max){
		   
		   int mask = 1 << 31;
	  	   if(hashcode < 0){
	  		    hashcode = mask ^ hashcode;
	  	   }
	       int  partition_index = hashcode % max;
		   
	       return partition_index;
	  }
	
	  
		public int inputSplit(String inputfile, int sizeblock, String splitfile_path){
			
			int linecount = 0;   
			String line = null;
			int block = 0;
			final int BLOCK_ERROR = -1;
			BufferedReader reader = null;
			try {

				reader = new BufferedReader(new FileReader(inputfile));
				String splitfile = splitfile_path + block + ".txt";
				PrintWriter writer = new PrintWriter(new FileWriter(splitfile, false));
				FM.CreateNewFile(splitfile);
			
				while(( line = reader.readLine() ) != null) {
				
					linecount++;

					FM.WriteRecord(line, writer);
					
					if(sizeblock == linecount){
						block++;
						writer.close();
						splitfile = splitfile_path + block + ".txt";
						FM.CreateNewFile(splitfile);
						writer = new PrintWriter(new FileWriter(splitfile, false));
						linecount = 0;
					}
				}
				writer.close();
				reader.close();
				
				return block;
			} catch  (FileNotFoundException e) {
			    e.printStackTrace();
			    return BLOCK_ERROR;
			} catch  (IOException e) {
				System.err.println(e);
				return BLOCK_ERROR;
			}
			
		}
}