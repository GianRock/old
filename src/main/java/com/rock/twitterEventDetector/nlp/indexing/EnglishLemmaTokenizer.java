package com.rock.twitterEventDetector.nlp.indexing;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.Morphology;
import edu.stanford.nlp.process.PTBTokenizer.PTBTokenizerFactory;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EnglishLemmaTokenizer extends Tokenizer {
	private Morphology morph;
	private TaggedWord currentWord;
	private CharTermAttribute termAttr;
	private PayloadAttribute payloadAttr;
	private final PartOfSpeechAttribute posAttr;
	private Iterator<TaggedWord> tagged;
	private Reader myReader;
	private final OffsetAttribute offsetAttr;
	private MaxentTagger tagger;

	public EnglishLemmaTokenizer(MaxentTagger tagger) {

		// TODO Auto-generated constructor stub
		super();
		this.tagger = tagger;

		this.termAttr = addAttribute(CharTermAttribute.class);
		payloadAttr = addAttribute(PayloadAttribute.class);
		this.offsetAttr = addAttribute(OffsetAttribute.class);
		this.posAttr=addAttribute(PartOfSpeechAttribute.class);
		this.morph = new Morphology();
	}

	private void initializeIterator() {
		try {
			this.myReader = new StringReader(toString(input));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DocumentPreprocessor dp = new DocumentPreprocessor(this.myReader);
		dp.setTokenizerFactory(PTBTokenizerFactory
				.newCoreLabelTokenizerFactory("untokenizable=noneKeep,ptb3Escaping=false,normalizeFractions=false,normalizeSpace=true"));
		List<TaggedWord> tagWords = new ArrayList<TaggedWord>();

		for (List<HasWord> sentence : dp) {
			List<TaggedWord> tSentence = this.tagger.tagSentence(sentence);
			for (TaggedWord taggedWord : tSentence) {
				tagWords.add(taggedWord);
			}
		}
		tagged = tagWords.iterator();
	}

	private String toString(Reader reader) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		int ch;
		while ((ch = reader.read()) > -1) {
			stringBuilder.append((char) ch);
		}
		return stringBuilder.toString();
	}

	@Override
	public boolean incrementToken() throws IOException {

		if (this.tagged == null) {
			initializeIterator();
		}
		// TODO Auto-generated method stub
		if (tagged.hasNext()) {
			clearAttributes();
			currentWord = tagged.next();
			String tag = currentWord.tag();
			 
			
			/**
			 * assegno un boost per quei token corrispondenti
			 * a Nomi propri o verbi
			 */
			if(tag.equals("NNP") || tag.equals("NNPS") 
				
			){
				if(currentWord.word().length()>1)
					payloadAttr.setPayload(new BytesRef(PayloadHelper.encodeFloat(1.5F)));
			}else{
				payloadAttr.setPayload(null);
			}
				//payloadAttr.setPayload(new BytesRef(PayloadHelper.encodeFloat(0.5F)) );
			 
			String form = currentWord.word();
			posAttr.setPartOfSpeech(tag);
			String lemma = morph.lemma(form, tag);
			if (lemma != null) {
				termAttr.append(lemma);
			} else {
				termAttr.append(currentWord.word());
			}

			offsetAttr.setOffset(currentWord.beginPosition(),currentWord.endPosition());

			return true;
		} else {
			return false;
		}

	}

	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		super.reset();
		this.tagged = null;

	}

	@Override
	public void end() throws IOException {
		// TODO Auto-generated method stub
		try {
			super.end();
			if(offsetAttr!=null)
				offsetAttr.setOffset(currentWord.beginPosition(),currentWord.endPosition());
		} catch (NullPointerException e) {
			// TODO: handle exception
			System.out.println(" aa ");
		}
		
	}
}
