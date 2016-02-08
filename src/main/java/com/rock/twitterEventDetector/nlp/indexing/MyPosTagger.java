package com.rock.twitterEventDetector.nlp.indexing;

import com.google.common.collect.Iterables;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.tagger.maxent.TaggerConfig;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

public class MyPosTagger  {
	

	private static class LazyHolder{
        private static final MyPosTagger INSTANCE = new MyPosTagger();

	}
	public static MyPosTagger getInstance(){
		return LazyHolder.INSTANCE;
	}
	public static String MODEL_FILE = "./nlp/gate-EN-twitter.model";
	public MaxentTagger tagger;
	public MyPosTagger(){
		//"untokenizable=noneKeep,ptb3Escaping=false,normalizeFractions=false,normalizeSpace=true"
		TaggerConfig config = new TaggerConfig("-model", MODEL_FILE,"-untokenizable","noneKeep","-ptb3Escaping","false");
	 
			tagger =new MaxentTagger(MODEL_FILE, config, false);
		 
	}
	/**
	 * 
	 * @param sentence
	 * @return
	 */
	public Iterator<TaggedWord> tagSentence(String sentence){
		StringReader input=new StringReader(sentence);
		 
		List<List<HasWord>> tokenized = MaxentTagger.tokenizeText(input);
		return  Iterables.concat(tagger.process(tokenized)).iterator();
	}
	 
	
	
	 
	/**
	 * 
	 * @param sentence
	 * @return
	 */
	public TreeMap<Integer,TaggedWord> tagSentenenceToMap(String sentence){
 		
		
 		TreeMap<Integer,TaggedWord> posMap=new TreeMap<Integer, TaggedWord>();

		
		StringReader input=new StringReader(sentence);
 	    
		
 
 		List<List<HasWord>> tokenized = MaxentTagger.tokenizeText(input);
 	
		Iterator<TaggedWord> taggedWords=  Iterables.concat(tagger.process(tokenized)).iterator();
		while(taggedWords.hasNext()){
			TaggedWord tagWord=taggedWords.next();
			posMap.put(tagWord.beginPosition(), tagWord);
		}
		return posMap;
		
	}
	
	public static void main(String[] args) {
		 
	    String text="Mini random flash mob of Gangnam Style in our seats while the speaker is talking hahahahahah I love my school.   love drive my Mini! pmEsani. ";
		String tperfett = "MedMob: Miami, Florida, USA is hosting a special Meditation Flash Mob, this Cristhmas 5pm at Bayfront Park!";
		String timesSquare = "Can we skip to the part where Michael Bublé proposes to me in Times Square in the middle of! a flashmob?";
		String k="Rare Disease Day FlashMob tomorrow! Use RDD2015/hashtag for your raredisease. :S :-P dystonia {} [] ()";
		String love="Sometimes I wanna be a successful college educated individual but most ot the times I wanna say fug the world and become a flash mob coordinator";
		String just="I WAS JUST IN A FLASHMOB";
		String energy="That energy flashmob looked crazy!!";
		String softBall=":-P  :-) -.- Our cheer/softball {} :P flashmob after school :))) ";
		Iterator<TaggedWord> posMap=  MyPosTagger.getInstance().tagSentence(love);
		 while (posMap.hasNext()) {
			TaggedWord taggedWord = posMap.next();
			System.out.println(taggedWord.beginPosition() +" "+taggedWord.word() +" "+taggedWord.tag());
			System.out.println(" substring " +love.substring(taggedWord.beginPosition(), taggedWord.endPosition()));
		}
		 
		 
	 TreeMap<Integer, TaggedWord>  treeMap=MyPosTagger.getInstance().tagSentenenceToMap(love);
	 for (Entry<Integer, TaggedWord> entry :  treeMap.entrySet()) {
		System.out.println(entry.getKey() +" "+ " "+ entry.getValue().word()+" "+entry.getValue().tag());
	}
	 
		 // option #1: By sentence.
 	     
	      
		/*DbPediaAnnotator d = new DbPediaAnnotator();
		List<DbPediaAnnotation> lista = d.annotate(timesSquare);
		for (DbPediaAnnotation dbPediaAnnotation : lista) {
		
			String surface= dbPediaAnnotation.getSurfaceText();
			String postag=posMap.get(dbPediaAnnotation.getStart()).tag();
			System.out.println(dbPediaAnnotation.getSurfaceText()+ " start"+dbPediaAnnotation.getStart()+" postag "+postag);

			List<List<HasWord>> tokenized = MaxentTagger.tokenizeText(new StringReader(surface));
			Iterable<HasWord> words=Iterables.concat(tokenized);
			for (HasWord word : words) {
				System.out.println("WORD "+word.word());
			}
 		}*/
	      
	    
	      
	      
	}
}
