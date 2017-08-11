package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class WordEntity {
	@PrimaryKey
	String word;
	List<String> countList;
	
	public WordEntity(){
		
	}
	
	public WordEntity(String word){
		this.word = word;
		this.countList = new ArrayList<String>();
	}
	
	public String getWord(){
		return word;
	}
	
	public List<String> getCountList(){
		return countList;
	}
	
	public void addCount(String value){
		countList.add(value);
	}
}
