package com.rock.twitterEventDetector.nlp.indexing;

import org.apache.lucene.util.AttributeImpl;

public class PartOfSpeechAttributeImpl extends AttributeImpl implements PartOfSpeechAttribute {
	private String pos=null;
	@Override
	public void setPartOfSpeech(String pos) {
		// TODO Auto-generated method stub
		this.pos=pos;
	}

	@Override
	public String getPartOfSpeech() {
		// TODO Auto-generated method stub
		return pos;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		pos=null;
	}

	@Override
	public void copyTo(AttributeImpl target) {
		// TODO Auto-generated method stub
		 ((PartOfSpeechAttribute) target).setPartOfSpeech(pos);
	}

}
