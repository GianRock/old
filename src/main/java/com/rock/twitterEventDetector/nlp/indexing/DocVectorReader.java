package com.rock.twitterEventDetector.nlp.indexing;

import com.rock.twitterFlashMobDetector.configuration.Constant;
import com.rock.twitterFlashMobDetector.db.DataMapper;
import com.rock.twitterFlashMobDetector.db.mongoDB.TweetsCollection;
import com.rock.twitterFlashMobDetector.model.annotation.SemanticAnnotation;
import com.rock.twitterFlashMobDetector.model.twitter.Entity;
import com.rock.twitterFlashMobDetector.model.twitter.MyTweet;
import edu.stanford.nlp.ling.TaggedWord;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

public class DocVectorReader {
	private static class LazyHolder {
		private static final DocVectorReader INSTANCE = new DocVectorReader();
	}

	public static DocVectorReader getInstance() {
		return LazyHolder.INSTANCE;
	}

	private class GetRelativeDocFrequency implements Callable<Float> {
		private String term;
		private int lowerDay;
		private int upperDay;

		GetRelativeDocFrequency(String term, int lowerDay, int upperDay) {
			this.term = term;
			this.lowerDay = lowerDay;
			this.upperDay = upperDay;

		}

		@Override
		public Float call() throws Exception {
			// TODO Auto-generated method stub
			int tdf = getTermDocumentFrequencyInDayRange(term, "text",
					lowerDay, upperDay);

			int df = getDocumentFrequencyInDayRange(lowerDay, upperDay);
			if (df > 1) {
				return ((float) (tdf + 1)) / ((float) (df + 1));
			} else
				return null;

		}

	}

	/**
	 * 
	 * @param term
	 * @param lowerDay
	 * @param upperDay
	 * @return
	 * @throws Exception
	 */
	private Float getRelativeDocFrequency(String term, int lowerDay,
			int upperDay, int df) throws Exception {
		int tdf = 0;
		tdf = getTermDocumentFrequencyInDayRange(term, "text", lowerDay,
				upperDay);
		// df = getDocumentFrequencyInDayRange(lowerDay,upperDay);

		if (df > 1) {
			return ((float) (tdf + 1)) / ((float) (df + 1));
		} else
			return null;
	}

	/**
	 * intero che rappresenta la distanza in numero di giorni dell'inizio della
	 * raccolta dei tweet rispetto al time epoch.
	 */
	public static int DAY_START = 16401;// 16133;

	/**
	 * dimensione della finestra temporale espressa in numero di giorni
	 */
	public static int TIME_WINDOW_SIZE = 7;

	/**
	 * dimensione di uno slot della fienestra temporale espressa in numero di
	 * giorni
	 * 
	 */
	public static int DAY_TIME_INTERVAL_WIDTH = 1;
	private IndexReader indexReader;
	private IndexSearcher indexSearcher;

	public DocVectorReader() {

		try {
			Directory dir = FSDirectory.open(Paths.get(Constant.INDEX_DIR));
			indexReader = DirectoryReader.open(dir);
			indexSearcher = new IndexSearcher(indexReader);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public DocVectorReader(IndexWriter writer) {

		try {
			this.indexReader = DirectoryReader.open(writer, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		indexSearcher = new IndexSearcher(indexReader);

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
	public DocVector getTermVectorMap(MyTweet tweet) {
		int ndoc = getDocumentIdByIdTweet(tweet.getId());
		if (ndoc != -1) {

			int dayNumber = (int) (tweet.getCreateAt().getTime() / 24 / 3600000);
			DocVector docVector;
			int numberOfToken = 0;
			TFIDFSimilarity sim = new DefaultSimilarity();
			try {
				Map<String, Float> tfIdfVectorMap = new HashMap<String, Float>();

				// System.out.println(ndoc);
				// indexReader=DocVectorReader.getIndexReader();

				Terms termVector = indexReader.getTermVector(ndoc, "text");

				if (termVector != null) {
					List<Integer> documentsNumberPerDay = getDocumentNumberOfTimeWindow(dayNumber);
					int documentNumbersInTimeWindow = 0;
					/*
					 * for (Integer dayDocNumber : documentsNumberPerDay) {
					 * documentNumbersInTimeWindow+=dayDocNumber; }
					 */
					documentNumbersInTimeWindow = documentsNumberPerDay
							.get(documentsNumberPerDay.size() - 1);
					TermsEnum termsEnum = termVector.iterator();
					BytesRef text;
					int dft = 1;

					int docFreq = 1;
					int collectionNumbers = -1;
					while ((text = termsEnum.next()) != null) {
						PostingsEnum postingsEnum = termsEnum.postings(null,
								null, PostingsEnum.ALL);

						if (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
							numberOfToken++;
							Float booostNounVerb = 1.0F;
							postingsEnum.nextPosition();
							int startOffset = postingsEnum.startOffset();
							int endOffset = postingsEnum.endOffset();

							BytesRef payload = postingsEnum.getPayload();
							if (payload != null) {
								booostNounVerb = PayloadHelper.decodeFloat(
										payload.bytes, payload.offset);
							}

							try {
								//
								if (collectionNumbers == -1) {

									collectionNumbers = getDocumentFrequencyInDayRange(
											0, dayNumber + 1);
								}

								dft = getTermDocumentFrequencyInDayRange(
										text.utf8ToString(), "text", dayNumber,
										dayNumber + 1);
								// docFreq =
								// getTermDocumentFrequencyInDayRange(text.utf8ToString(),
								// "text", 0,dayNumber+1);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// indexReader.docFreq(new Term("text", text));

							float idf = sim.idf(docFreq, collectionNumbers);
							// float tf = (float) ((float)(dft +0.5) / (float)
							// (documentNumbersInTimeWindow+1));
							float tf = sim.tf(postingsEnum.freq());
							// float timeBoost
							// =calculateTimeBoost("text",text.utf8ToString(),
							// dayNumber,documentsNumberPerDay);
							float timeBoost = 1f;
							if (tweet.getHashtags() != null
									&& tweet.getHashtags().size() > 0) {
								if (endOffset > tweet.getCleanedText().length()) {
									booostNounVerb = 1.5F;
								} else {

									String surfaceText = tweet.getCleanedText()
											.substring(startOffset, endOffset);
									for (Entity ht : tweet.getHashtags()) {
										if (ht.getText().equalsIgnoreCase(
												surfaceText)) {
											booostNounVerb = 2F;
											break;
										}
									}
								}
							}
							// System.out.println(text.utf8ToString()+
							// " TIME BOOOST " + timeBoost);
							// System.out.println("START OFFSET  "
							// +postingsEnum.startOffset()
							// +" "+postingsEnum.endOffset()
							// +" "+text.utf8ToString() +" doc freq "+docFreq
							// +" time boost " +timeBoost +" noun boost "
							// +booostNounVerb);

							tfIdfVectorMap.put(text.utf8ToString(), tf * idf
									* booostNounVerb * timeBoost);
						}

					}

				}

				docVector = new DocVector(tfIdfVectorMap);
				docVector.setNumberOfToken(numberOfToken);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				docVector = null;
			}
			return docVector;
		} else
			return null;
	}

	private List<Integer> getDocumentNumberOfTimeWindow(int dayNumber) {
		// TODO Auto-generated method stub
		int dayStart = (dayNumber - 7 < 0) ? DAY_START : (dayNumber - 7);
		int interval = (dayNumber - dayStart) / DAY_TIME_INTERVAL_WIDTH;
		List<Integer> documentNumbers = new ArrayList<Integer>();
		for (int i = 0; i < interval; i++) {
			try {
				documentNumbers.add(getDocumentFrequencyInDayRange((i)
						* DAY_TIME_INTERVAL_WIDTH + dayStart, (i + 1)
						* DAY_TIME_INTERVAL_WIDTH + dayStart));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return documentNumbers;
	}

	/**
	 * 
	 * @param fieldName
	 * @param documentsNumberPerDay
	 * @param string
	 * @return
	 */
	private Float calculateTimeBoost22(String fieldName, String termText,
			int dayNumber, List<Integer> documentsNumberPerDay) {
		// TODO Auto-generated method stub
		// DAY_START=dayNumber-TIME_INTERVAL_WIDTH*10;
		int dayStart = (dayNumber - 7 < 0) ? DAY_START : (dayNumber - 7);
		int interval = (dayNumber - dayStart) / DAY_TIME_INTERVAL_WIDTH;
		if (interval <= 1) {
			return 1F;
		} else {
			try {
				float dfi = getTermDocumentFrequencyInDayRange(termText,
						fieldName, (interval) * DAY_TIME_INTERVAL_WIDTH
								+ dayStart, (interval + 1)
								* DAY_TIME_INTERVAL_WIDTH + dayStart);

				int documentFreq = documentsNumberPerDay
						.get(documentsNumberPerDay.size() - 1);
				float rdfi = (dfi + 1) / (documentFreq + 1);
				// rdfi=1.0f;
				List<Float> termFrequencies = new ArrayList<Float>();
				// ExecutorService executor =
				// Executors.newFixedThreadPool(interval);
				// List<Future<Float>> futureWeights=new
				// ArrayList<Future<Float>>();
				// for(int i=interval-1;i>0;i--){
				for (int i = 0; i < interval - 1; i++) {
					/*
					 * futureWeights.add(
					 * 
					 * executor.submit(new GetRelativeDocFrequency(termText, (i)
					 * TIME_INTERVAL_WIDTH + dayStart, (i+1) TIME_INTERVAL_WIDTH
					 * + dayStart)));
					 */
					termFrequencies.add(getRelativeDocFrequency(termText, (i)
							* DAY_TIME_INTERVAL_WIDTH + dayStart, (i + 1)
							* DAY_TIME_INTERVAL_WIDTH + dayStart,
							documentsNumberPerDay.get(i)));
				}
				// executor.shutdown();
				/*
				 * for (Future<Float> future : futureWeights) {
				 * if(future.get()!=null) termFrequencies.add(future.get()); }
				 */
				// termFrequencies.add(rdfi);
				Float ema = ExponentialMovingAverage.ema(termFrequencies);
				// System.out.println("NUMERO DI TWEET bello stesso periodo  "+documentFreq);
				// System.out.println("fieldName  " + fieldName +" term  : "
				// +termText + " :"+ dfi);
				// System.out.println("fieldName  " + fieldName +" term "
				// +termText + " relative doc freq : "+ rdfi);
				// System.out.println(termText+ " EMA "+ema);

				Float timeBoost = rdfi / ema;
				System.out.println("time boost" + timeBoost);
				// timeBoost = timeBoost < 1f ? 1f : timeBoost;
				// timeBoost = timeBoost > 1.5f ? 1.5f : timeBoost;
				// System.out.println("tb " +timeBoost);
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
	 * 
	 * @param fieldName
	 * @param documentsNumberPerDay
	 * @param string
	 * @return
	 */
	private Float calculateTimeBoost(String fieldName, String termText,
			int dayNumber, List<Integer> documentsNumberPerDay) {
		// TODO Auto-generated method stub
		// DAY_START=dayNumber-TIME_INTERVAL_WIDTH*10;
		int dayStart = (dayNumber - 7 < 0) ? DAY_START : (dayNumber - 7);
		int interval = (dayNumber - dayStart) / DAY_TIME_INTERVAL_WIDTH;
		if (interval <= 1) {
			return 1F;
		} else {
			try {
				float dfi = getTermDocumentFrequencyInDayRange(termText,
						fieldName, (interval) * DAY_TIME_INTERVAL_WIDTH
								+ dayStart, (interval + 1)
								* DAY_TIME_INTERVAL_WIDTH + dayStart);

				int documentFreq = documentsNumberPerDay
						.get(documentsNumberPerDay.size() - 1);
				float rdfi = (dfi + 1) / (documentFreq + 1);
				// rdfi=1.0f;
				List<Float> termFrequencies = new ArrayList<Float>();
				// ExecutorService executor =
				// Executors.newFixedThreadPool(interval);
				// List<Future<Float>> futureWeights=new
				// ArrayList<Future<Float>>();
				// for(int i=interval-1;i>0;i--){

				float termFreqsTotal = 0F;
				for (int i = 0; i < interval - 1; i++) {
					/*
					 * futureWeights.add(
					 * 
					 * executor.submit(new GetRelativeDocFrequency(termText, (i)
					 * TIME_INTERVAL_WIDTH + dayStart, (i+1) TIME_INTERVAL_WIDTH
					 * + dayStart)));
					 */

					termFreqsTotal += getRelativeDocFrequency(termText, (i)
							* DAY_TIME_INTERVAL_WIDTH + dayStart, (i + 1)
							* DAY_TIME_INTERVAL_WIDTH + dayStart,
							documentsNumberPerDay.get(i));

				}
				termFreqsTotal = termFreqsTotal / (interval - 1);
				// executor.shutdown();
				float timeBoost = (float) (rdfi / (Math.log(termFreqsTotal + 1) + 1));
				return timeBoost;
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

		NumericRangeQuery<Integer> queryRange = NumericRangeQuery.newIntRange(
				"day", lowerDay, upperDay, true, false);
		BooleanQuery bq = new BooleanQuery();
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
	 * 
	 * @param fieldName
	 * @param lowerDay
	 * @param upperDay
	 * @return
	 * @throws Exception
	 */
	private int getDocumentFrequencyInDayRange(int lowerDay, int upperDay)
			throws Exception {
		// System.out.println(" intervallo : "+lowerDay +" "+upperDay);
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
			 * try { Document d=indexSearcher.doc(td.scoreDocs[0].doc);
			 * System.out.println(d.get("text")); } catch (IOException e) { //
			 * TODO Auto-generated catch block e.printStackTrace(); }
			 */
			return td.scoreDocs[0].doc;

		} else
			return -1;

	}

	public static void main(String[] args) {

		MyPosTagger posTagger = MyPosTagger.getInstance();
		Long idTweet = 435928018020597761L;

		MyTweet t1 = TweetsCollection.findTweetById(idTweet);
		System.out.println(t1.getText());
		System.out.println(t1.getCreateAt());
		DocVectorReader doCReader = new DocVectorReader();
		DocVector d = doCReader.getTermVectorMap(t1,
				posTagger.tagSentenenceToMap(t1.getCleanedText()));
		Map<String, Float> vector = d.getMapVector();
		for (Entry<String, Float> entry : vector.entrySet()) {
			System.out.println(entry.getKey() + " weight " + entry.getValue());
		}
		System.out.println(d.getNumberOfToken());
		System.out.println(t1.getCleanedText());

		List<Integer> freqs = doCReader.getDocumentNumberOfTimeWindow(16564);

		List<MyTweet> tweets = TweetsCollection.findAllTweets();
		for (MyTweet myTweet : tweets) {
			System.out
					.println(myTweet.getId() + " " + myTweet.getCleanedText());
			DocVector ds = doCReader.getTermVectorMap(myTweet,
					posTagger.tagSentenenceToMap(myTweet.getCleanedText()));
			Map<String, Float> v = ds.getMapVector();
			for (Entry<String, Float> entry : v.entrySet()) {
				System.out.println(entry.getKey() + " weight "
						+ entry.getValue());
			}
		}

		// System.out.println(t.substring(28,32));
	}

	public static void mains(String[] args) {
		List<MyTweet> tweets = TweetsCollection.findAllTweets();// ts.findTweetsOfClusterExecution(445);
		for (MyTweet t1 : tweets) {
			DocVectorReader t = new DocVectorReader();
			DocVector d = t.getTermVectorMap(t1);
			System.out.println("id " + t1.getId() + "*****************");
			System.out.println("text " + t1.getCleanedText());
			System.out.println(" NUMERO TOKENS : " + d.getNumberOfToken());
			DataMapper.getInstance().insertNumberTokens(t1.getId(),
					d.getNumberOfToken());
			/*
			 * Map<String,Float> m=d.getMapVector(); Set<String>
			 * terms=m.keySet();
			 * System.out.println("TWEET DATA CREAZIONE  "+t1.getCreateAt());
			 * for (String term : terms) { System.out.println(term
			 * +" "+m.get(term) +" weight "+m.get(term)); }
			 */
		}

	}

	public static void main2(String[] args) throws IOException {

		MyTweet t1 = TweetsCollection.findTweetById(547809603069964289L);
		MyTweet t2 = TweetsCollection.findTweetById(556632420037197824L);
		System.out.println(t1.getCreateAt());
		DocVectorReader t = new DocVectorReader();
		DocVector d = t.getTermVectorMap(t1);
		DocVector d2 = t.getTermVectorMap(t2);

		System.out.println("SIM  " + d.cosineSimilarity(d2));
		Set<String> topk = d.getTopKKeywords(5);
		for (String string : topk) {
			System.out.println(string);
		}
		topk = d2.getTopKKeywords(5);
		System.out.println("TOP K KEYOWrDS DI D2");
		for (String string : topk) {
			System.out.println(string);
		}
		DocVector d23 = d2.average(d);
		topk = d23.getTopKKeywords(10);
		System.out.println("TOP K KEYOWrDS DI D223");
		for (String string : topk) {
			System.out.println(string);
		}
		System.out.println(d.getNumberOfToken());
		System.out.println(d2.getNumberOfToken());
		System.out.println(d23.getNumberOfToken());
	}

	public DocVector getTermVectorMap(MyTweet tweet,
			Set<SemanticAnnotation> semanticAnnotationSet) {
		int dayNumber = (int) (tweet.getCreateAt().getTime() / 24 / 3600000);
		DocVector docVector;
		TFIDFSimilarity sim = new DefaultSimilarity();
		try {
			Map<String, Float> tfIdfVectorMap = new HashMap<String, Float>();
			int ndoc = getDocumentIdByIdTweet(tweet.getId());

			// System.out.println(ndoc);
			// indexReader=DocVectorReader.getIndexReader();
			if (ndoc != -1) {
				Terms termVector = indexReader.getTermVector(ndoc, "text");

				if (termVector != null) {

					TermsEnum termsEnum = termVector.iterator();
					BytesRef text;
					while ((text = termsEnum.next()) != null) {
						PostingsEnum postingsEnum = termsEnum.postings(null,
								null, PostingsEnum.PAYLOADS);

						if (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
							postingsEnum.nextPosition();
							Float boostAnnotationOrHashTag = 1.0F;
							int startOffset = postingsEnum.startOffset();
							int endOffset = postingsEnum.endOffset();
							if (semanticAnnotationSet != null
									&& semanticAnnotationSet.size() > 0) {

								if (isAnnotation(startOffset, endOffset,
										semanticAnnotationSet)) {
									boostAnnotationOrHashTag = 1.5F;
								}
							}
							if (tweet.getHashtags() != null
									&& tweet.getHashtags().size() > 0) {
								String surfaceText = tweet.getCleanedText()
										.substring(startOffset, endOffset);
								for (Entity ht : tweet.getHashtags()) {
									if (ht.equals(surfaceText)) {
										boostAnnotationOrHashTag = 1.5F;
										break;
									}
								}
							}

							// System.out.println("START OFFSET  "
							// +postingsEnum.startOffset()
							// +" "+postingsEnum.endOffset()
							// +" "+text.utf8ToString());
							float idf = sim.idf(termsEnum.docFreq(),
									indexReader.numDocs());
							float tf = sim.tf(postingsEnum.freq());
							float timeBoost = 1f;
							// float timeBoost =
							// calculateTimeBoost("text",text.utf8ToString(),
							// dayNumber,);
							System.out.println(text.utf8ToString()
									+ " TIME BOOOST " + timeBoost);
							tfIdfVectorMap.put(text.utf8ToString(), tf * idf
									* boostAnnotationOrHashTag * timeBoost);
						}

					}

				}
			}

			docVector = new DocVector(tfIdfVectorMap);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			docVector = null;
		}
		return docVector;
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
			if (startOffset >= semanticAnnotation.getStart()
					&& endOffset <= (semanticAnnotation.getStart() + semanticAnnotation
							.getSurfaceText().length()))
				return true;

		}

		return false;
	}

	/**
	 * 
	 * @param tweet
	 * @param posTags
	 * @return
	 */
	public DocVector getTermVectorMap(MyTweet tweet,
			TreeMap<Integer, TaggedWord> posTagMap) {
		// TODO Auto-generated method stub
		int ndoc = getDocumentIdByIdTweet(tweet.getId());
		if (ndoc != -1) {

			int dayNumber = (int) (tweet.getCreateAt().getTime() / 24 / 3600000);
			DocVector docVector;
			int numberOfToken = 0;
			TFIDFSimilarity sim = new DefaultSimilarity();
			try {
				Map<String, Float> tfIdfVectorMap = new HashMap<String, Float>();

				// System.out.println(ndoc);
				// indexReader=DocVectorReader.getIndexReader();

				Terms termVector = indexReader.getTermVector(ndoc, "text");

				if (termVector != null) {
					// List<Integer >
					// documentsNumberPerDay=getDocumentNumberOfTimeWindow(dayNumber);
					// int documentNumbersInTimeWindow = 0;
					/*
					 * for (Integer dayDocNumber : documentsNumberPerDay) {
					 * documentNumbersInTimeWindow+=dayDocNumber; }
					 */

					// documentNumbersInTimeWindow=documentsNumberPerDay.get(documentsNumberPerDay.size()-1);
					TermsEnum termsEnum = termVector.iterator();
					BytesRef text;

					int docFreq = 1;
					int collectionNumbers = -1;
					while ((text = termsEnum.next()) != null) {
						PostingsEnum postingsEnum = termsEnum.postings(null,
								null, PostingsEnum.ALL);

						if (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
							numberOfToken++;
							Float booostNounVerb = 1.0F;
							Float boostHashTag=1.0f;
							postingsEnum.nextPosition();
							int startOffset = postingsEnum.startOffset();
							int endOffset = postingsEnum.endOffset();
							int lenghtOfText = tweet.getCleanedText().length();

							if (startOffset < tweet.getCleanedText().length())

								/*
								 * BytesRef payload = postingsEnum.getPayload();
								 * if (payload != null) { booostNounVerb =
								 * PayloadHelper.decodeFloat( payload.bytes,
								 * payload.offset); }
								 */

								if (tweet.getHashtags() != null
										&& tweet.getHashtags().size() > 0) {
									if (endOffset > tweet.getCleanedText()
											.length()) {
										boostHashTag = 2F;
									} else {

										String surfaceText = tweet
												.getCleanedText().substring(
														startOffset, endOffset);

										for (Entity ht : tweet.getHashtags()) {
											if (ht.getText().equalsIgnoreCase(
													surfaceText)) {
												boostHashTag = 2F;
												break;
											}
										}
									}
								}
							if (startOffset < lenghtOfText){
								
								/**
								 * se il token non è un hashtag controllo se 
								 * è un nome o un verbo.
								 */
								if(boostHashTag<2f){
									booostNounVerb = getBoostFromPosTag(startOffset, endOffset, posTagMap);
								}
							}
								

							try {
								//
								if (collectionNumbers == -1) {

									collectionNumbers = getDocumentFrequencyInDayRange(0, dayNumber + 1);
								}

								 
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// indexReader.docFreq(new Term("text", text));

							float idf = sim.idf(docFreq, collectionNumbers);
							// float tf = (float) ((float)(dft +0.5) / (float)// (documentNumbersInTimeWindow+1));
							float tf = sim.tf(postingsEnum.freq());
							// float timeBoost
							// =calculateTimeBoost("text",text.utf8ToString(),
							// dayNumber,documentsNumberPerDay);
							//float timeBoost = 1f;

							tfIdfVectorMap.put(text.utf8ToString(), tf * idf* booostNounVerb * boostHashTag);
						}

					}

				}

				docVector = new DocVector(tfIdfVectorMap);
				docVector.setNumberOfToken(numberOfToken);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				docVector = null;
			}
			return docVector;
		} else
			return null;
	}

	/**
	 * 
	 * @param startOffset
	 * @param endOffset
	 * @param posTagMap
	 * @return
	 */
	private Float getBoostFromPosTag(int startOffset, int endOffset,
			TreeMap<Integer, TaggedWord> posTagMap) {
		// TODO Auto-generated method stub

		float posTagBoost = 1f;
		if (posTagMap.containsKey(startOffset)) {
			String tag = posTagMap.get(startOffset).tag();
			if (tag.startsWith("NN")) {

				posTagBoost = 1.2f;
				if (tag.equals("NNP") || tag.equals("NNPS"))
					posTagBoost =1.5f;

			} else if (tag.startsWith("VB")) {
				posTagBoost = 1.2f;
			}
		} else {
			System.err.println(" non cè il pos tag per  la poszione "
					+ startOffset + " " + endOffset);
		}
		return posTagBoost;
	}

}
