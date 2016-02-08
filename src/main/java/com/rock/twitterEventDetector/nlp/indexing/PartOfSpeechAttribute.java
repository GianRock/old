package com.rock.twitterEventDetector.nlp.indexing;

import org.apache.lucene.util.Attribute;

public interface PartOfSpeechAttribute extends Attribute {
   
    void setPartOfSpeech(String pos);
  
   
    String getPartOfSpeech();
     
  }