package com.rock.twitterEventDetector.nlp.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeSource;
import scala.Tuple2;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class AnalyzerUtils {

	public static List<Tuple2<String,String>> lematizeText(EnglishLemmaAnalyzer analyzer,
			String text) throws IOException {
		 List<Tuple2<String,String>> listTokensPosTags = new ArrayList<Tuple2<String,String>>();
		if (text != null) {
			System.out.println(text);
			// System.out.println(text);
			TokenStream stream = // 1
			analyzer.tokenStream("contents", new StringReader(text));
			CharTermAttribute cattr = stream
					.addAttribute(CharTermAttribute.class);
			PartOfSpeechAttribute posAttribute = stream
					.addAttribute(PartOfSpeechAttribute.class);
			stream.reset();
			while (stream.incrementToken()) {
				
				listTokensPosTags.add(new Tuple2<String,String>(cattr.toString(),posAttribute.getPartOfSpeech()));
			}
			stream.end();
			stream.close();
		}
		return listTokensPosTags;
	}

	public static List<String> tokenizeText(Analyzer analyzer, String text)
			throws IOException {
		List<String> listTokens = new ArrayList<String>();
		if (text != null) {
			// System.out.println(text);
			TokenStream stream = // 1
			analyzer.tokenStream("contents", new StringReader(text));
			CharTermAttribute cattr = stream
					.addAttribute(CharTermAttribute.class);
			stream.reset();
			while (stream.incrementToken()) {
				listTokens.add(cattr.toString());
			}
			stream.end();
			stream.close();
		}
		return listTokens;
	}

	/**
	 * 
	 * @param text
	 * @return
	 * @throws IOException
	 */
	public static List<String> tokenizeText(String text) throws IOException {
		Analyzer analyzer = new MyAnalyzer();
		List<String> listTokens = new ArrayList<String>();
		if (text != null) {
			// System.out.println(text);
			TokenStream stream = // 1
			analyzer.tokenStream("contents", new StringReader(text));
			CharTermAttribute cattr = stream
					.addAttribute(CharTermAttribute.class);
			stream.reset();
			while (stream.incrementToken()) {
				listTokens.add(cattr.toString());
			}
			stream.end();
			stream.close();
			analyzer.close();
		}
		return listTokens;

	}

	public static AttributeSource[] tokensFromAnalysis(Analyzer analyzer,
													   String text) throws IOException {
		TokenStream stream = // 1
		analyzer.tokenStream("contents", new StringReader(text)); // 1
		ArrayList<AttributeSource> tokenList = new ArrayList<AttributeSource>();

		CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);
		OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);

		stream.reset();
		while (stream.incrementToken()) {

			System.out.println(cattr.toString() + " offset :"
					+ offset.startOffset() + " - " + offset.endOffset());
		}
		stream.end();
		stream.close();
		return tokenList.toArray(new AttributeSource[0]);
	}

	public static void displayTokens(Analyzer analyzer, String text)
			throws IOException {
		byte ptext[] = text.getBytes();
		text = new String(ptext, "UTF-8");
		AttributeSource[] tokens = tokensFromAnalysis(analyzer, text);
		for (int i = 0; i < tokens.length; i++) {
			AttributeSource token = tokens[i];
			CharTermAttribute term = token
					.addAttribute(CharTermAttribute.class);
			System.out.print("[cazz" + term.toString() + "] "); // 2
		}
	}

	public static void main(String[] args) {
		 
		String t = "AlexSalmond If our Flashmob video gets more views than your visit to CentrestageMT video (70K) will you post a dancing selfie? fionacs";
		String usaf = "Wow! Thanks for share   The USAF band Flash Mob at Smithsonian Air & space museum. Pretty darn cool. USAF";
		try {
			AnalyzerUtils.tokensFromAnalysis(new StandardAnalyzer(),
					usaf);
			
			
			List<Tuple2<String,String>> tokPostags=AnalyzerUtils.lematizeText(new EnglishLemmaAnalyzer(), usaf);
			tokPostags.forEach(tuple->System.out.println(tuple._1+ " pos tag "
					+tuple._2));
			// AnalyzerUtils.displayTokens(new EnglishLemmaAnalyzer(), t);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}