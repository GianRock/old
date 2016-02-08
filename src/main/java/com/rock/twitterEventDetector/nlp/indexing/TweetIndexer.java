package com.rock.twitterEventDetector.nlp.indexing;

import com.rock.twitterFlashMobDetector.configuration.Constant;
import com.rock.twitterFlashMobDetector.db.mongoDB.TweetsCollection;
import com.rock.twitterFlashMobDetector.model.twitter.MyTweet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * questa classe gestisce l'indice di lucene contente i tweet
 * 
 * @author rocco
 *
 */
public class TweetIndexer {
	static   int count=0;
	private static class LazyHolder {
		private static final TweetIndexer INSTANCE = new TweetIndexer();
	}
 	//private StanfordNerAnnotator nerAnnotator=new StanfordNerAnnotator();
	public static TweetIndexer getInstance(){
		return LazyHolder.INSTANCE;
	}
	private IndexWriter indexWriter;
	private IndexReader indexReader;
	public IndexWriter getIndexWriter() {
		return indexWriter;
	}

	public void setIndexWriter(IndexWriter indexWriter) {
		this.indexWriter = indexWriter;
	}
	private IndexSearcher searcher;
	private TweetIndexer() {
		// TODO Auto-generated constructor stub4
		
		opendIndex();
		

 	 
	}
 	 
	/**
	 * 
	 * @return
	 */
	private Document createTweetDocument(MyTweet tweet) {
		// TODO Auto-generated method stub
		Document d = new Document();
		Field id = new StringField("id",tweet.getId().toString(), Store.YES);
		d.add(id);
		FieldType fd=new FieldType();
		fd.setTokenized(true);
		fd.setStoreTermVectors(true);
	//	fd.setStoreTermVectorPayloads(true);
		fd.setStoreTermVectorPositions(true);
		fd.setStoreTermVectorOffsets(true);
		fd.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		fd.setStored(true);
		
		 
		Field text=new Field("text",tweet.getCleanedText(),fd);

 		d.add(text);
 		 
 		
 		Field timeCreation=new LongField("time_creation", tweet.getCreateAt().getTime(), Store.YES);
		d.add(timeCreation);
 		//d.add(new NumericField("time_creation").setLongValue(tweet.getCreateAt().getTime()));
		int day = (int) (tweet.getCreateAt().getTime() / 24 / 3600000);
		Field dayField=new IntField("day", day, Store.YES);
		d.add(dayField);
		//d.add(new IntField("day", Store.YES, true).setIntValue(day));
		//System.out.println(tweet.getId() + " DAY " + day);
		/*
		if (isImportant(tweet)) {
			d.setBoost(1.2F);
		}*/

		return d;
	}
	 
	/**
	 * aggiunge un tweet all'indice
	 * 
	 * @param tweet
	 */
	public void addTweetToIndex(MyTweet tweet) {
		try {
			indexWriter.addDocument(createTweetDocument(tweet));
			indexWriter.commit();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void opendIndex(){
		IndexWriterConfig iwc = new IndexWriterConfig(new MyAnalyzer());
		 iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		 Directory dir = null;
		try {
		 
				dir = FSDirectory.open(Paths.get(Constant.INDEX_DIR));
				
			 
			 iwc.setRAMBufferSizeMB(2048.0);
			 this.indexWriter=new IndexWriter(dir, iwc);
			 this.indexReader= DirectoryReader.open(this.indexWriter,false);
			 this.searcher=new IndexSearcher(indexReader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * aggiunge un insieme di tweet all'indice
	 * @param tweetSet
	 */
	public void addTweetsToIndex(Collection<MyTweet> tweets){
 		 
		
		if(!this.indexWriter.isOpen()){
			opendIndex();
		}
		ExecutorService executor = Executors.newFixedThreadPool(256);//(256);
		boolean added=false;
 		for (MyTweet tweet : tweets) {
 			if(!existInIndex(tweet)){
				executor.execute(new AddTweetToIndex(tweet));
				added=true;
 			}
 		 
		}
 		executor.shutdown();
		try {
			executor.awaitTermination(tweets.size(), TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		if(added)
			this.commitChangesToIndex();
 	 /*
		 
		int i=0;
		for (MyTweet tweet : tweets) {

			 if (!existInIndex(tweet)) {
				try {
					indexWriter.addDocument(createTweetDocument(tweet));
					//ts.setIndexed(tweet.getId());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }

			i++;
			System.out.println("tweet " + i);
		} */
	}
	/**
	 * 
	 * @param tweet
	 * @return
	 */
	private boolean existInIndex(MyTweet tweet) {
		// TODO Auto-generated method stub
		
		Term t = new Term("id", tweet.getId().toString());
		Query query = new TermQuery(t);
		 
		TopDocs td = null;
		try {
			td = searcher.search(query, 1);
 		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return td != null && td.totalHits > 0;

	}
	private void optimizeAndClose() {
		// TODO Auto-generated method stub
		try {
 			//this.indexWriter.commit();
			this.indexReader.close();
  			
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void closeIndex()
	{
		try {
			this.indexReader.close();
			this.indexWriter.close();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * esegue un commit 
	 */
	public void commitChangesToIndex(){
		try {
			this.indexWriter.commit();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		
		 
		//	MyTweet t=tweetService.findTweetById(468637447518044160L);
		List<MyTweet> allTweets=TweetsCollection.findAllTweets();
		//allTweets=allTweets.subList(0, 100);
 		System.out.println(allTweets.size());
 		 TweetIndexer indexer = TweetIndexer.getInstance();
 		Long timeStart=System.currentTimeMillis();

 		 indexer.addTweetsToIndex(allTweets);
		 
		 indexer.optimizeAndClose();
		Long timeEnd=System.currentTimeMillis();
		Long timeEx=timeEnd-timeStart;
		System.out.println("TEMPO eX "+timeEx); 
	 
	}
	 
	/**
	 * 
	 * @author rocco
	 *
	 */
	class AddTweetToIndex implements Runnable{
		
		private MyTweet tweet;
		public AddTweetToIndex(MyTweet tweet) {
			// TODO Auto-generated constructor stub
			this.tweet=tweet;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				// TODO Auto-generated method stub
				Document d = new Document();
				Field id = new StringField("id",tweet.getId().toString(), Store.YES);
				d.add(id);
				FieldType fd=new FieldType();
				fd.setTokenized(true);
				fd.setStoreTermVectors(true);
				fd.setStoreTermVectorPayloads(true);
				fd.setStoreTermVectorPositions(true);
				fd.setStoreTermVectorOffsets(true);
				fd.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
				fd.setStored(true);
				
				 
				String tweetText=tweet.getCleanedText();
				if(tweet.getSplittedHashtags()!=null)
					tweetText+=" "+tweet.getSplittedHashtags();
				Field text=new Field("text",tweetText,fd);

		 		d.add(text);
		 		 d.add(new LongField("time_creation", tweet.getCreateAt().getTime()/3600000, Store.YES));
		 		
		 	 
				//d.add(new NumericField("time_creation").setLongValue(tweet.getCreateAt().getTime()));
				int day = (int) (tweet.getCreateAt().getTime() / 24 / 3600000);
				Field dayField=new IntField("day", day, Store.YES);
				d.add(dayField);
			   indexWriter.addDocument(d);
			   count++;
			   System.out.println(" count "+count);
 			 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
