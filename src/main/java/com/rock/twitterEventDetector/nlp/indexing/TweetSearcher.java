package com.rock.twitterEventDetector.nlp.indexing;

import com.rock.twitterFlashMobDetector.model.twitter.MyTweet;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TweetSearcher {
	private static class LazyHolder{
        private static final TweetSearcher INSTANCE = new TweetSearcher();

	}
	public static TweetSearcher getInstance(){
		return LazyHolder.INSTANCE;
	}
	private IndexSearcher searcher;
	private TweetSearcher() {
		// TODO Auto-generated constructor stub
		try {
			Directory dir= FSDirectory.open(Paths.get(com.rock.twitterFlashMobDetector.configuration.Constant.INDEX_DIR));
			searcher=new IndexSearcher(DirectoryReader.open(dir));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @param keyWord
	 */
	public List<MyTweet> searchs(String keyWord,Collection<MyTweet> tweets) {
 		List<MyTweet> resultTweets=new ArrayList<MyTweet>();
		TermQuery textTermQuery=new TermQuery(new Term("text", keyWord));
	//	TermQuery hashTagTermQuery=new TermQuery(new Term("hashtag", keyWord));
		//TermQuery namedEntityTermQuery=new TermQuery(new Term("namedEntity", keyWord));
	
		//booleanQuery.add(hashTagTermQuery, Occur.SHOULD);
		//booleanQuery.add(namedEntityTermQuery, Occur.SHOULD);
		 BooleanQuery mainQuery=new BooleanQuery();
		String[] idas=new String[tweets.size()];
		int i=0;
		List<BytesRef> ids=new ArrayList<BytesRef>();
		for (MyTweet myTweet : tweets) {
			BooleanQuery booleanQuery=new BooleanQuery();
		 	 booleanQuery.add(textTermQuery, Occur.MUST);
		 TermQuery termId=new TermQuery(new Term("id", myTweet.getId().toString()));
		//System.out.println(termId.toString());
		 ids.add(new BytesRef(myTweet.getId().toString()));
 		//booleanQueryID.add(termId,Occur.FILTER);
		 booleanQuery.add(new BooleanClause(termId ,  Occur.FILTER));
		idas[i]=""+myTweet.getId();
		i++;
		mainQuery.add(booleanQuery, Occur.SHOULD);
		}
		System.out.println("QUERY "+mainQuery.toString());
  	 QueryWrapperFilter qw=new QueryWrapperFilter(mainQuery);
		
		
		try {
			TopDocs topDocs=searcher.search( mainQuery,Integer.MAX_VALUE);
 			ScoreDoc[] scoreDocs=topDocs.scoreDocs;
 			System.out.println(scoreDocs.length);
			for (ScoreDoc scoreDoc : scoreDocs) {
				Document d=searcher.doc(scoreDoc.doc);
				Long idTweet=Long.parseLong(d.get("id"));
				
				for (MyTweet t : tweets) {
					if(t.getId().equals(idTweet))
						resultTweets.add(t);
						
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultTweets;


		
	}
	
	/**
	 * 
	 * @param keyWord
	 */
	public List<Long> searchsIds(String keyWord,Collection<MyTweet> tweets) {
 		List<Long> resultTweets=new ArrayList<Long>();
		TermQuery textTermQuery=new TermQuery(new Term("text", keyWord));
	//	TermQuery hashTagTermQuery=new TermQuery(new Term("hashtag", keyWord));
		//TermQuery namedEntityTermQuery=new TermQuery(new Term("namedEntity", keyWord));
	
		//booleanQuery.add(hashTagTermQuery, Occur.SHOULD);
		//booleanQuery.add(namedEntityTermQuery, Occur.SHOULD);
		 BooleanQuery mainQuery=new BooleanQuery();
		String[] idas=new String[tweets.size()];
		int i=0;
		List<BytesRef> ids=new ArrayList<BytesRef>();
		for (MyTweet myTweet : tweets) {
			BooleanQuery booleanQuery=new BooleanQuery();
		 	 booleanQuery.add(textTermQuery, Occur.MUST);
		 TermQuery termId=new TermQuery(new Term("id", myTweet.getId().toString()));
		//System.out.println(termId.toString());
		 ids.add(new BytesRef(myTweet.getId().toString()));
 		//booleanQueryID.add(termId,Occur.FILTER);
		 booleanQuery.add(new BooleanClause(termId ,  Occur.FILTER));
		idas[i]=""+myTweet.getId();
		i++;
		mainQuery.add(booleanQuery, Occur.SHOULD);
		}
		System.out.println("QUERY "+mainQuery.toString());
 		
		
		try {
			TopDocs topDocs=searcher.search( mainQuery,Integer.MAX_VALUE);
 			ScoreDoc[] scoreDocs=topDocs.scoreDocs;
 			System.out.println(scoreDocs.length);
			for (ScoreDoc scoreDoc : scoreDocs) {
				Document d=searcher.doc(scoreDoc.doc);
				Long idTweet=Long.parseLong(d.get("id"));
				
				resultTweets.add(idTweet);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultTweets;


		
	}
 }
