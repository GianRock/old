package com.rock.twitterEventDetector.nlp.indexing;

import com.rock.twitterFlashMobDetector.db.mongoDB.TweetsCollection;
import com.rock.twitterFlashMobDetector.model.twitter.MyTweet;
import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.List;
import java.util.regex.Pattern;

public class AnalyzerDemo {
	  private static final String SPACE_EXCEPTIONS = "\\n\\r";
	  public static final String SPACE_CHAR_CLASS = "\\p{C}\\p{Z}&&[^" + SPACE_EXCEPTIONS + "\\p{Cs}]";
	  public static final String SPACE_REGEX = "[" + SPACE_CHAR_CLASS + "]";

	  public static final String PUNCTUATION_CHAR_CLASS = "\\p{P}\\p{M}\\p{S}" + SPACE_EXCEPTIONS;
	  public static final String PUNCTUATION_REGEX = "[" + PUNCTUATION_CHAR_CLASS + "]";
 	  private static final String EMOTICON_DELIMITER =
	           SPACE_REGEX + "|" + PUNCTUATION_REGEX;

	  public static final Pattern SMILEY_REGEX_PATTERN = Pattern.compile(":[)DdpPSsoO]+|:[ -]\\)|<3+");
	  public static final Pattern FROWNY_REGEX_PATTERN = Pattern.compile(":[(<]|:[ -]\\(");
 	  public static final Pattern EMOTICON_REGEX_PATTERN =
	          Pattern.compile("(?<=^|" + EMOTICON_DELIMITER + ")("
	            + SMILEY_REGEX_PATTERN.pattern() + "|" + FROWNY_REGEX_PATTERN.pattern()
	            + ")+(?=$|" + EMOTICON_DELIMITER + ")");
	private static final String[] examples = {
 
		"how can i get involved with the flashmob wi-fi  ?",
"TWITTER FLASHMOB: Favorite the tweet but don't RT! Reply if you want. She missed the man of her dreams  for tv/party ",
"EVERYBODY FAVORITE AND REPLY \"NOPE\" TO THIS TWEET FLASHMOB stopBombingGaza lol ahahahhahahahahaha "
	};
	
 
 		private static final Analyzer[] analyzers = new Analyzer[] {
		
		
		//new SimpleAnalyzer(),
 
		// new MyAnalyzer(),
 		new MyAnalyzer(),
		 new EnglishLemmaAnalyzer()
	
		//a
		 
	 
		};
	 
		
		class Demo implements Runnable{
			String text;
			public Demo(String text) {
				// TODO Auto-generated constructor stub
				this.text=text;
			}
			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.out.println("Analyzing \"" + text + "\"");
				for (int i = 0; i < analyzers.length; i++) {
				Analyzer analyzer = analyzers[i];
				String name = analyzer.getClass().getName();
				name = name.substring(name.lastIndexOf(".") + 1);
				System.out.println(" " + name + ":");
				 
				try {
					AnalyzerUtils.displayTokens(analyzer, text);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("\n");
				}
				}
			 
			
		}
		
		
		public static void main(String[] args) throws IOException {
			// Use the embedded example strings, unless
			// command line arguments are specified, then use those.
			String[] strings = examples;
			 
			 if (args.length > 0) {
			strings = args;
			}
			for (int i = 0; i < strings.length; i++) {
				analyze(strings[i]);
			}
			
		 
			 
		
		
		}
		private static void analyze(String text) throws IOException {
			// TODO Auto-generated method stub
			
			//MoreLikeThis m=new MoreLikeThis(IndexReader.open(null, false));
			
			System.out.println("Analyzing \"" + text + "\"");
			for (int i = 0; i < analyzers.length; i++) {
			Analyzer analyzer = analyzers[i];
			String name = analyzer.getClass().getName();
			name = name.substring(name.lastIndexOf(".") + 1);
			System.out.println(" " + name + ":");
		 
			AnalyzerUtils.displayTokens(analyzer, text);
			System.out.println("\n");
			}
			}
		
		/**
		 * 
		 * @param text
		 * @return
		 */
		public static String removeAccents(String text) {
		     		return text == null ? null :
		        Normalizer.normalize(text, Form.NFD)
		            .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
		}
		
		public static void main2(String[] args) {
			String accented="aáeéiíoóöőuúüű AÁEÉIÍOÓÖŐUÚÜŰ"
					;
		//	ZemantaAnnotationService annotator=new ZemantaAnnotationService();
 
			String norma=  Normalizer.normalize(accented, Form.NFD);//"The flashmob proposal they did on Grey's 😍😍😍"
			String unacc=removeAccents(norma);
			System.out.println(unacc);
 			List<MyTweet> tweets=TweetsCollection.findAllTweets();
			int i=0;
			for (MyTweet myTweet : tweets) {
				String cleanedText=myTweet.getCleanedText();
				String utf8tweet = "";
 		            byte[] utf8Bytes;
					try {
						utf8Bytes = cleanedText.getBytes("UTF-8");
						 utf8tweet = new String(utf8Bytes, "UTF-8");

					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

		           
		        
		      
				String newCleanedText=utf8tweet.replaceAll("\\p{So}+", "");
				if(!cleanedText.equals(newCleanedText)){
					// ts.updateCleanedText(myTweet.getId(), newCleanedText);
					i++;
					 System.out.printf(" Il tweet id:%d%n contiene delle faccine  %n", myTweet.getId());
					//System.out.println("OLD TEXT "+cleanedText);
					//System.out.println("NEW  TEXT "+newCleanedText);
					 
			} 
		}
			System.out.println("ci sono "+i+" stringhe contenenti le faccine");
			String with="can we please do a flash mob to this😂 just me and you😂";
			byte[] utf8Bytes;
			try {
				utf8Bytes = with.getBytes("UTF-8");
				with = new String(utf8Bytes, "UTF-8");
				String newCleanedText=with.replaceAll("\\p{So}+", "");
				System.out.println(newCleanedText);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

}
}