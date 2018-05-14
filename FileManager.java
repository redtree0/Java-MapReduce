package mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
//import java.util.TreeMap;

public class FileManager {
	
	public void WriteFiles(Map<String, Integer>combined, String combinefile){
		try{
			CreateNewFile(combinefile);
			PrintWriter writer = new PrintWriter(new FileWriter(combinefile, false));
			for(Map.Entry<String, Integer> entry : combined.entrySet()) {
			    String key = entry.getKey();
			    Integer value = entry.getValue();
			    System.out.println(key + ", " + value);
			    WriteRecord(key + ", " + value, writer);
			}
			writer.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	  public void ClosePrintWriters(List<PrintWriter> partitionlists){
		  for(PrintWriter pw : partitionlists){
			  pw.close();
		  }
	  }
	  
	  public List<PrintWriter> CreatePrintWriters(String filename, int partition_max, boolean append) throws IOException{
		  String partition = null;
		  List<PrintWriter> partition_lists = new ArrayList<PrintWriter>();
		  
		  for(int i=0; i< partition_max; i++){
			  partition = filename + i + ".txt";
			  CreateNewFile(partition);
			  PrintWriter writer = new PrintWriter(new FileWriter(partition, append));
			  partition_lists.add(writer);
		  }
  
		  return partition_lists;
	  }
	  
	  
	  public void CreateNewFile(String nfile){
		  	File wfile = new File(nfile);    // 샘플 파일 생성
			
		  	try {
				wfile.createNewFile(); // 이름의 파일 생성
			} catch  (Exception e) {
			    	e.printStackTrace();
			    	System.out.println(nfile + " 파일 생성이 안됩니다. !!!");
			    	return;
			}
	  }
	  
	  public void RemoveFiles(String file){
		  File wfile = new File(file);
		  
		  try {
			  wfile.delete();
		  } catch  (Exception e) {
			    	e.printStackTrace();
			    	return;
		   }
	  }
	  
	  
	  public void WriteRecord(String words, PrintWriter writer) {
		try {

		    writer.println(words); 
		    
		} catch  (Exception ec) {
		    ec.printStackTrace();
		}
	  }
	  
}
