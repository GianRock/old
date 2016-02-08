package com.rock.twitterEventDetector.nlp.indexing;

import com.mongodb.client.MongoCollection;
import com.rock.twitterFlashMobDetector.db.mongoDB.MongoClientSingleton;
import com.rock.twitterFlashMobDetector.db.mongoDB.TweetsCollection;
import com.rock.twitterFlashMobDetector.model.annotation.SemanticAnnotation;
import com.rock.twitterFlashMobDetector.model.twitter.MyTweet;
import org.apache.lucene.index.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DocVectorReaderNumberOfToken {
	/**
	 * intero che rappresenta la distanza in numero di giorni dell'inizio della
	 * raccolta dei tweet rispetto al time epoch.
	 */
	public static int DAY_START = 16401;// 16133;
	/**
	 * dimensione della finestra temporale espressa in numero di giorni
	 */
	public static int TIME_INTERVAL_WIDTH = 1;
	private IndexReader indexReader;
	private IndexSearcher indexSearcher;
 
	public DocVectorReaderNumberOfToken() {

		try {
			Directory dir = FSDirectory.open(Paths.get(com.rock.twitterFlashMobDetector.configuration.Constant.INDEX_DIR));
			indexReader = DirectoryReader.open(dir);
			indexSearcher=new IndexSearcher(indexReader);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public DocVectorReaderNumberOfToken(IndexWriter writer) {

		 
		try {
			this.indexReader= DirectoryReader.open(writer,false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			indexSearcher=new IndexSearcher(indexReader);

		 

	}

	 

	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
 		indexReader.close();
		super.finalize();
	}

	/**
	 * Restiutisce il vettore tf-idf di un documento relativo ad un tweet,
	 * rappresentato mediante un dizionario <term,tf-idf>
	 * 
	 * @param idTweet
	 * @return null se l'id del tweet non � presente nell'indice (o vi � qualche
	 *         eccezzione) il vetore tf-idf del tweet, altrimenti.
	 * 
	 * @throws IOException
	 */
	public int getTermVectorMap(Long idTweet) {
  		int ntoken=0;
		try {
 			int ndoc = getDocumentIdByIdTweet(idTweet);

			// System.out.println(ndoc);
			// indexReader=DocVectorReader.getIndexReader();
			if (ndoc != -1) {
				 
				Terms termVector = indexReader.getTermVector(ndoc, "text");
				 
				if (termVector != null) {
					
					
					
					
					TermsEnum termsEnum= termVector.iterator();
					  BytesRef text;
					 while((text = termsEnum.next()) != null) {
						 
							   PostingsEnum postingsEnum=   termsEnum.postings(null, null, PostingsEnum.ALL);
							   
							   if(postingsEnum.nextDoc()!= DocIdSetIterator.NO_MORE_DOCS){
								   ntoken++;
							   }

					  }
					 
				}
			}

 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			 ntoken=-1;
		}
		return ntoken;
	}
 
	/**
	 * 
	 * @param fieldName
	 * @param string
	 * @return
	 */
	private Float calculateTimeBoost(String fieldName, String termText,
			int dayNumber) {
		// TODO Auto-generated method stub
		// DAY_START=dayNumber-TIME_INTERVAL_WIDTH*10;
		int dayStart=(dayNumber-7<0) ? DAY_START: (dayNumber-7) ;
		int interval = (dayNumber - dayStart) / TIME_INTERVAL_WIDTH;
 		if (interval <= 1) {
			return 1F;
		} else {
			try {
				float dfi = getTermDocumentFrequencyInDayRange(termText,
						fieldName,
						(interval) * TIME_INTERVAL_WIDTH + dayStart,
						(interval + 1) * TIME_INTERVAL_WIDTH + dayStart);

				int documentFreq = getDocumentFrequencyInDayRange((interval)
						* TIME_INTERVAL_WIDTH + dayStart, (interval + 1)
						* TIME_INTERVAL_WIDTH + dayStart);
				float rdfi = (dfi + 1) / (documentFreq + 1);
				 // rdfi=1.0f;
				List<Float> termFrequencies = new ArrayList<Float>();

				// for(int i=interval-1;i>0;i--){
				for (int i = 0; i < interval - 1; i++) {

					int tdf = getTermDocumentFrequencyInDayRange(termText,
							fieldName, (i) * TIME_INTERVAL_WIDTH + dayStart,
							(i + 1) * TIME_INTERVAL_WIDTH + dayStart);

					int df = getDocumentFrequencyInDayRange((i)
							* TIME_INTERVAL_WIDTH + dayStart, (i + 1)
							* TIME_INTERVAL_WIDTH + dayStart);
					// System.out.println(tdf+" ----df: "+df);
					float weight = ((float) (tdf + 1)) / ((float) (df + 1));
					// System.out.println("INTERVAL"+ i+" - "+termText
					// +"  "+fieldName +" :"+weight);
					termFrequencies.add(weight);
				}
				// termFrequencies.add(rdfi);
				Float ema = ExponentialMovingAverage.mean(termFrequencies);
				// System.out.println("NUMERO DI TWEET bello stesso periodo  "+documentFreq);
				// System.out.println("fieldName  " + fieldName +" term  : "
				// +termText + " :"+ dfi);
				// System.out.println("fieldName  " + fieldName +" term "
				// +termText + " relative doc freq : "+ rdfi);
				//System.out.println(termText+ " EMA "+ema);

				Float timeBoost = rdfi / ema;
				timeBoost= timeBoost>1.5f?1.5f:timeBoost;
				return timeBoost;

				// if(timeBoost>2f)
				// timeBoost=2f;
				// return timeBoost;

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 1f;
			}
		}

	}

	/**
	 * Restituisce il numero di tweet contenenti il termine specificato in input
	 * (testo,field) all'interno di una finestra temporale stabilito dai
	 * parametri [lowerday,upperday[
	 * 
	 * @param termText
	 * @param fieldName
	 * @param lowerDay
	 * @param upperDay
	 * @return
	 * @throws Exception
	 */
	public int getTermDocumentFrequencyInDayRange(String termText,
			String fieldName, int lowerDay, int upperDay) throws Exception {

		Term t = new Term(fieldName, termText);
		Query q = new TermQuery(t);

	 
		NumericRangeQuery<Integer> queryRange = NumericRangeQuery.newIntRange("day",
				lowerDay, upperDay, true, false);
		BooleanQuery bq=new BooleanQuery();
		bq.add(q, Occur.FILTER);
		 bq.add(queryRange, Occur.FILTER);
 		TopDocs matches = this.indexSearcher.search(bq, Integer.MAX_VALUE);
		
		// System.out.println(matches.totalHits);
		return matches.totalHits;

	}

	/**
	 * Restituisce il numero di tweet all'interno di una finestra temporale
	 * stabilito dai parametri [lowerday,upperday[
	 * 
	 * @param termText
	 * @param fieldName
	 * @param lowerDay
	 * @param upperDay
	 * @return
	 * @throws Exception
	 */
	private int getDocumentFrequencyInDayRange(int lowerDay, int upperDay)
			throws Exception {

		NumericRangeQuery<Integer> query = NumericRangeQuery.newIntRange("day",
				lowerDay, upperDay, true, false);

		 
 		TopDocs matches = this.indexSearcher.search(query, Integer.MAX_VALUE);
		// System.out.println(matches.totalHits);
		return matches.totalHits;

	}

	/**
	 * dato l'id di un tweet resituisce il relativo document number dell'indice
	 * 
	 * @param idTweet
	 * @return
	 */
	private int getDocumentIdByIdTweet(Long idTweet) {
		Term t = new Term("id", idTweet.toString());
		Query query = new TermQuery(t);
	 
		TopDocs td = null;
		try {
			td = this.indexSearcher.search(query, 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (td != null && td.totalHits > 0) {
			
			/*
			try {
				Document d=indexSearcher.doc(td.scoreDocs[0].doc);
				System.out.println(d.get("text"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
 			return td.scoreDocs[0].doc;
 			

		} else
			return -1;

	}

	public static void main(String[] args) {
		Long idTweet = 607766036926971904L;
	 
 		MyTweet t1 = TweetsCollection.findTweetById(idTweet);
		DocVectorReaderNumberOfToken doCReader = new DocVectorReaderNumberOfToken();
		int d = doCReader.getTermVectorMap(t1.getId());
		 System.out.println(d);
		System.out.println(t1.getCleanedText());
		String t=t1.getCleanedText();
		 Float timeBoost=doCReader.calculateTimeBoost("text", "kitemmurt", 16594);
		 System.out.println(timeBoost);
		//System.out.println(t.substring(28,32));
		 MongoCollection<Document> collection=MongoClientSingleton.getInstance().getDatabase("twitter").getCollection("tokenNumbers");
		 
		 List<MyTweet> allTweets=TweetsCollection.findAllTweets();
		 for (MyTweet myTweet : allTweets) {
			 collection.insertOne(new org.bson.Document().append("_id", myTweet.getId(  )).append("num_token", doCReader.getTermVectorMap(myTweet.getId())));
			//System.out.println(myTweet.getCleanedText() +" NTOKEN :"+doCReader.getTermVectorMap(myTweet.getId()) );
		}
	}

	/**
	 * 
	 * @param startOffset
	 * @param endOffset
	 * @param semanticAnnotationSet
	 * @return
	 */
	private boolean isAnnotation(int startOffset, int endOffset,
			Set<SemanticAnnotation> semanticAnnotationSet) {
		// TODO Auto-generated method stub
		for (SemanticAnnotation semanticAnnotation : semanticAnnotationSet) {
			if(startOffset==semanticAnnotation.getStart() 
					&& endOffset==(semanticAnnotation.getStart()+semanticAnnotation.getSurfaceText().length())
					)
				return true;
				
		}
		
		return false;
	}

}
