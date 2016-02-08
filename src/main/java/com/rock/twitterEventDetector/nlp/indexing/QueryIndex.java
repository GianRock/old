package com.rock.twitterEventDetector.nlp.indexing;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class QueryIndex {
	private EnglishLemmaAnalyzer analyzer=new EnglishLemmaAnalyzer();
	private IndexReader indexReader;
	private IndexSearcher indexSearcher;
	public QueryIndex() {
	
		  try {
			  Directory dir = FSDirectory.open(Paths
						.get(com.rock.twitterFlashMobDetector.configuration.Constant.INDEX_DIR+"2"));
			indexReader = DirectoryReader.open(dir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  indexSearcher = new IndexSearcher(indexReader);
		// TODO Auto-generated constructor stub
	}
	public List<Long> getIdsOfQuery(String query){
		 QueryParser parser = new QueryParser("text", analyzer);
		Query q=null;
		List<Long> ids=new ArrayList<Long>();
		try {
			q=parser.parse(query);
			TopDocs matches = null;
			 
				matches = indexSearcher.search(q, Integer.MAX_VALUE);
			 
			 ScoreDoc[] hits = matches.scoreDocs;
			 for (ScoreDoc scoreDoc : hits) {
				//System.out.println(scoreDoc.doc+" "+scoreDoc.score+" "+indexReader.document(scoreDoc.doc).get("id"));
				ids.add(Long.parseLong(indexReader.document(scoreDoc.doc).get("id")));
			 
			 }
		} catch (ParseException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 return ids;
		
	}
	public static void main23(String[] args) throws IOException {
		Directory dir = FSDirectory.open(Paths
				.get(com.rock.twitterFlashMobDetector.configuration.Constant.INDEX_DIR+"2"));
		IndexReader indexReader = DirectoryReader.open(dir);
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		TermQuery tq=new TermQuery(new Term("text", "baltimore"));
		TopDocs matches = indexSearcher.search(tq, Integer.MAX_VALUE);
		 ScoreDoc[] hits = matches.scoreDocs;
		 for (ScoreDoc scoreDoc : hits) {
			System.out.println(scoreDoc.doc+" "+scoreDoc.score+" "+indexReader.document(scoreDoc.doc).get("id"));
		}
	}
	public static void main(String[] args) {
		QueryIndex qi=new QueryIndex();
		List<Long> ids=qi.getIdsOfQuery("BaltimoreRiots rioting baltimore");
		for (Long long1 : ids) {
			System.out.println(long1);
		}
	}
}
