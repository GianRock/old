package com.rock.twitterEventDetector.nlp.indexing;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;

import java.io.IOException;
import java.util.regex.Pattern;

public class PartOfSpeechTaggingFilter extends FilteringTokenFilter {
 	public static final Pattern REPEATED_REGEX= Pattern.compile("([a-zA-Z]+)\\1{2,}");

	private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
	private final CharTermAttribute termAtt=addAttribute(CharTermAttribute.class);
 	public PartOfSpeechTaggingFilter(TokenStream in) {
		super(in);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected boolean accept() throws IOException {
		// TODO Auto-generated method stub
		//termAtt.toString().
		return termAtt.toString().length()>2 && !(REPEATED_REGEX.matcher(termAtt.toString()).find()) && !unwantedPOS(posAtt.getPartOfSpeech()) ;//&& !(REPEATED_REGEX.matcher(termAtt.toString()).matches());
	}
	private static final Pattern unwantedPosRE = Pattern
			.compile("^(HT|USR|CC|[LR]RB|POS|PRP|UH|WDT|WP|WP\\$|WRB|\\$|\\#|\\(|\\)|\\{|\\}|[|]|\\.|\\,|:|''|``|>|SYM)$");

	/**
	 * Determines if words with a given POS tag should be omitted from the
	 * index. Defaults to filtering out punctuation and function words
	 * (pronouns, prepositions, "the", "a", etc.).
	 *
	 * @see <a
	 *      href="http://www.ims.uni-stuttgart.de/projekte/CorpusWorkbench/CQP-HTMLDemo/PennTreebankTS.html">The
	 *      Penn Treebank tag set</a> used by Stanford NLP
	 */
	protected boolean unwantedPOS(String tag) {
 	//  return false;
	  return unwantedPosRE.matcher(tag).matches();
	}
 }
