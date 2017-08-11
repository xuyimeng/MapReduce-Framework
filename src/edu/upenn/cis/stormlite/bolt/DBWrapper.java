package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class DBWrapper {
	
	private String rootDir = null;
	private Environment myEnv;
	private EntityStore store;
	
	private PrimaryIndex<String,WordEntity> wordIndex;
	
	public DBWrapper(String dir){
		rootDir = dir;
		
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		// check if the file exists in given root directory
		File myfile = new File(rootDir);
		if(!myfile.exists()) {
			myfile.mkdirs();
			System.out.println("new directory created for DB...");
		}
		myEnv = new Environment(myfile,envConfig);
		
		//prepare and create entity store
		StoreConfig stConfig = new StoreConfig();
		stConfig.setAllowCreate(true);
		stConfig.setTransactional(true);
		store = new EntityStore(myEnv,"EntityStore",stConfig);
		
		//initialize the word entity
		wordIndex = store.getPrimaryIndex(String.class, WordEntity.class);
		System.out.println("Berkeley DB has setted up...");
	}
	
	public void syncDB() {
		if(store != null){
			store.sync();
		}
		if(myEnv != null){
			myEnv.sync();
		}
	}
	
	public void closeDB(){
		if(store != null){
			store.close();
		}
		if(myEnv != null){
			myEnv.close();
		}
	}

	
	public boolean containsWord(String word){
		return wordIndex.contains(word);
	}
	
	public List<String> getCountList(String word){
		WordEntity wordEntity = wordIndex.get(word);
		return wordEntity.getCountList();
	}
	
	public void addNewWord(String word,String count){
		WordEntity wordEntity = new WordEntity(word);
		wordEntity.addCount(count);
		wordIndex.put(wordEntity);
		System.out.println("New word "+word+" add to db with count "+count);
	}
	
	public void addCountToWord(String word,String count){
		WordEntity wordEntity = wordIndex.get(word);
		wordEntity.addCount(count);
		wordIndex.put(wordEntity);
	}
	
	public List<String> getWordList(){
		List<String> wordList = new ArrayList<>();
		for(String word : wordIndex.keys()){
			wordList.add(word);
		}
		return wordList;
	}
}
