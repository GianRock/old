package com.rock.twitterEventDetector.nlp.indexing;

import java.util.*;

/**
 * Questa classe rappresenta un vetore della matrice termini documenti,
 * mediante un hashmap. questa rappresetanzione � molto pi� compatta a causa
 * della sparsit� della matrice, e permette una maggiore scalabilit�
 * @author rocco
 *
 */
public class DocVector {
	 /**
	  * 
	  */
	private Map<String,Float> mapVector;
	private int numberOfToken;
	public Map<String, Float> getMapVector() {
		return mapVector;
	}
	/**
	 * norma euclidea del vettore
	 */
	private Double norm;
	public DocVector(Map<String,Float> mapVector) {
		// TODO Auto-generated constructor stub
		this.mapVector=mapVector;
		calculateL2Norm();
	}
	private void calculateL2Norm(){
		Collection<Float> values=this.mapVector.values();
		Double norma=0.0;
		for (Float value : values) {
			norma+=value*value;
		}
		this.norm =Math.sqrt(norma);
	}
	public Double getNorm() {
		return norm;
	}
	/**
	 * /** calcola il coseno dell'angolo compreso fra i due vettori,
	 * a causa della grande dimensionalit� dei dati nella matrice termini documenti
	 * questi vettori sono stati implementati mediante dei dizionari che permettono una
	 * rappresenazione pi� compatta
	 * 
	 * <img src="http://upload.wikimedia.org/math/8/e/f/8ef8b2ef8d12d056912422df398d7c92.png">
	 
	 * @param that
	 * @return
	 */
	public Double cosineSimilarity(DocVector that){
		Double sim=0.0;
		if(this.mapVector.size()>0 && that.mapVector.size()>0)
			sim=	this.dotProduct(that) /( (this.norm*that.norm));
		if(sim>1.0)
			sim=1.0;
		
		return sim;
		 
		
	}
	public Set<String> getTopKKeywords(int k){
		Map<String,Float> orderedMap=sortByValue(this.mapVector);
		int i=0;
		Set<String> topK=new HashSet<String>();
		for (String keyWord : orderedMap.keySet()) {
			if(i<k)
				topK.add(keyWord);
			else
				break;
			i++;
			
		}
		return topK;
	}
	public  int getNumberOfToken(){
		return this.numberOfToken;
	}
	public void setNumberOfToken(int numberOfToken){
		this.numberOfToken=numberOfToken;
	}
	public DocVector average(DocVector that){
		Map<String,Float> newDocVector=new HashMap<String, Float>();
		Set<String> thisKeyWords=new HashSet<String>(this.mapVector.keySet());
		Set<String> thatKeyWords=new HashSet<String>(that.mapVector.keySet());
		for (String keyword : thatKeyWords) {
			Float freq=0F;
			if(thisKeyWords.contains(keyword)){
				  freq=(this.mapVector.get(keyword)+that.mapVector.get(keyword))/2;
				thisKeyWords.remove(keyword);
			
			}else{
				freq=that.mapVector.get(keyword);
			}
			newDocVector.put(keyword, freq);
		}
		for (String keyword : thisKeyWords) {
			Float freq=0F;
			 
				freq=this.mapVector.get(keyword);
			 
			newDocVector.put(keyword, freq);
		}
		return new DocVector(newDocVector);
 

		
	}
	/**
	 * calcola il prodotto interno fra i due array
	 * @param that
	 * @return
	 */
	private float dotProduct(DocVector that) {
		// TODO Auto-generated method stub
		Float innerProduct=0F;
		for (String term : this.mapVector.keySet()) {
			if(that.mapVector.containsKey(term)){
				//System.out.println("COMMON TERM "+ term);
				innerProduct+=this.mapVector.get(term)*that.mapVector.get(term);
			}
			
		}
 		return innerProduct;
	}
	
	public double getWeightOfToken(String token){
		double w= this.mapVector.get(token);
		return w;
	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V> 
    sortByValue( Map<K, V> map )
{
    List<Map.Entry<K, V>> list =
        new LinkedList<>( map.entrySet() );
    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
    {
        @Override
        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
        {
            return -(o1.getValue()).compareTo( o2.getValue() );
        }
    } );

    Map<K, V> result = new LinkedHashMap<>();
    for (Map.Entry<K, V> entry : list)
    {
        result.put( entry.getKey(), entry.getValue() );
    }
    return result;
}
 public static void main(String[] args) {
	String patternSplitCamel="(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])";
	String patternSplitCamelWithNumbers="(?<!(^|[A-Z0-9]))(?=[A-Z0-9])|(?<!d^)(?=[A-Z][a-z])";
	String[] parts="HappyBirthdayTomHiddleston".split(patternSplitCamelWithNumbers);
	for (String string : parts) {
		System.out.println(string);
	}
	Map<String, Float> a=new HashMap<String, Float>();
	a.put("a", 0.9F);
	a.put("b", 0.3F);
 
	a.put("c", 0.4F);
	a.put("d", 0.2F);
	a.put("e", 0.2F);

	Map<String, Float> b=sortByValue(a);
	for (String key : b.keySet()) {
		System.out.println(b.get(key));
	}
	 
}
}
