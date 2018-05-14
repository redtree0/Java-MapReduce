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
		int numPart = 10;  // 10���� ��Ƽ�� ���
		int sizeblock = 100;  // ��Ƽ���� ���� ��

		
		final String WORKSPACE = "C:\\Users\\CONSOLE\\Desktop\\tmp\\";
		String inputfile = WORKSPACE + "sample.txt";
		String splitfile = WORKSPACE + "split";
		String partfile =  WORKSPACE + "partition";
		String combinefile = WORKSPACE + "combined";
		String textreduce = WORKSPACE + "textreduce.txt";
	  	// ���ེ ��� ����
		String reducefile = WORKSPACE + "reduced.txt";  		// �󵵼��� ���ĵ� ���ེ ��� ����
		String filter = WORKSPACE + "wordfilter.txt"; 
		// �м� ���� �ܾ� ����
		String filted = WORKSPACE + "filted.txt"; 
		// ���͸� �� ���ེ ��� ����
			
		WCMapper map = new WCMapper();

		WCShuffler sh = new WCShuffler();
		WCReducer rd = new WCReducer();
		
		
		int block = map.inputSplit (inputfile, sizeblock, splitfile);
		System.out.println("@�Է� ������ " + block +"���� ���Ϸ� ���ø��Ǿ����ϴ�.");
//		System.out.println(block);
		for (int j=0; j<block; j++) {
			map.mapper(j, splitfile, numPart, partfile);
			System.out.println("@���ø� "+ j +"�� �ܾ 10���� ��Ƽ�ǿ� �־����ϴ�.");
		}

		Sleep(1);
		for (int j=0; j<numPart; j++){
			System.out.println("@���ÿ��� "+ j+ " ��Ƽ���� ����/���� �����Ͽ�, �޹��� ������ ��������ϴ�.");
			sh.sortCombiner(j, partfile, combinefile);
		}
		
		Sleep(1);
		int words = rd.reduceMerger(numPart, combinefile, textreduce);
		System.out.println(System.getProperty("line.separator")+"@���༭���� ���ེ ���� "+ textreduce+ "�� ����Ͽ����ϴ�.");
		System.out.println("@���յ� �ܾ���� " + words + "�� �Դϴ�.");
		
		Sleep(1);
		System.out.println("@���յ� �ܾ ���� ������������ �����Ͽ� ����մϴ�.");
		rd.sortFreq(textreduce, reducefile);
////
		Sleep(1);
		rd.filterWord(filter, reducefile, filted);
////		
		Sleep(1);
		int top_record_n = 50;
		System.out.println("@�󵵼� ���� "+ top_record_n +"�� ������ �ܾ ����մϴ�.");
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
