package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class WCMain {

	public static void main(String[] args) throws Exception {
		int numPart = 10;  // 10개의 파티션 사용
		int sizeblock = 100;  // 파티션의 라인 수

		
		final String WORKSPACE = "C:\\Users\\CONSOLE\\Desktop\\tmp\\";
		String inputfile = WORKSPACE + "sample.txt";
		String splitfile = WORKSPACE + "split";
		String partfile =  WORKSPACE + "partition";
		String combinefile = WORKSPACE + "combined";
		String textreduce = WORKSPACE + "textreduce.txt";
	  	// 리듀스 출력 파일
		String reducefile = WORKSPACE + "reduced.txt";  		// 빈도수로 정렬된 리듀스 출력 파일
		String filter = WORKSPACE + "wordfilter.txt"; 
		// 분석 제외 단어 모음
		String filted = WORKSPACE + "filted.txt"; 
		// 필터링 된 리듀스 출력 파일
			
		WCMapper map = new WCMapper();

		WCShuffler sh = new WCShuffler();
		WCReducer rd = new WCReducer();
		
		
		int block = map.inputSplit (inputfile, sizeblock, splitfile);
		System.out.println("@입력 파일이 " + block +"개의 파일로 스플릿되었습니다.");
//		System.out.println(block);
		for (int j=0; j<block; j++) {
			map.mapper(j, splitfile, numPart, partfile);
			System.out.println("@스플릿 "+ j +"의 단어를 10개의 파티션에 넣었습니다.");
		}

		Sleep(1);
		for (int j=0; j<numPart; j++){
			System.out.println("@셔플에서 "+ j+ " 파티션을 정렬/내부 병합하여, 콤바인 파일을 만들었습니다.");
			sh.sortCombiner(j, partfile, combinefile);
		}
		
		Sleep(1);
		int words = rd.reduceMerger(numPart, combinefile, textreduce);
		System.out.println(System.getProperty("line.separator")+"@리듀서에서 리듀스 파일 "+ textreduce+ "을 출력하였습니다.");
		System.out.println("@병합된 단어들은 " + words + "개 입니다.");
		
		Sleep(1);
		System.out.println("@통합된 단어를 빈도의 내림차순으로 정렬하여 출력합니다.");
		rd.sortFreq(textreduce, reducefile);
////
		Sleep(1);
		rd.filterWord(filter, reducefile, filted);
////		
		Sleep(1);
		int top_record_n = 50;
		System.out.println("@빈도수 상위 "+ top_record_n +"위 까지의 단어를 출력합니다.");
		rd.select(filted, top_record_n);

	}
	
	public static void Sleep(int time){
		 try {
		      Thread.sleep(time * 1000);
	    } catch (InterruptedException e) { }
	}
	
}


class Sortbyroll<T1 extends Comparable<T1>, T2 extends Comparable<T2>> implements Comparator<Pair<T1, T2>>
{
 
	@Override
	public int compare(Pair<T1, T2> o1, Pair<T1, T2> o2) {
		
		if(o1.GetKey().equals(o2.GetKey())){
			return o1.GetValue().compareTo(o2.GetValue());	
		}
		return 0;
	}

}

class SortbyValue<T1 extends Comparable<T1>, T2 extends Comparable<T2>> implements Comparator<Pair<T1, T2>>
{
 
	@Override
	public int compare(Pair<T1, T2> o1, Pair<T1, T2> o2) {
		
		return o1.GetValue().compareTo(o2.GetValue());
	}

}

class Pair<T1 extends Comparable<T1>, T2 extends Comparable<T2>> implements Comparable<Pair<T1, T2>>{
	
	private T1 key;
	private T2 value;
	
	
	Pair(T1 key, T2 value){
		this.key = key;
		this.value = value;
	}
	public T1 GetKey() {
		return this.key;
	}
	
	public T2 GetValue() {
		return this.value;
	}

	@Override
	public int compareTo(Pair<T1,T2> other) {
	  return GetKey().compareTo(other.GetKey());
	}
	
}
