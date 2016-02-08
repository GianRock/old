package com.rock.twitterEventDetector.nlp.indexing;

import java.util.LinkedList;
import java.util.List;

public class ExponentialMovingAverage {
		public static Float SMOOTHING_FACTOR=0.5f;
		public static float ema2(List<Integer> observations)
		{
			if(observations.size()==1){
				System.out.println(observations.get(0));
				return observations.get(0);
			}
				
			else{
		 
				
				return SMOOTHING_FACTOR*observations.get(observations.size()-1)+(1-SMOOTHING_FACTOR)*
						ema2(observations.subList(0, observations.size()-1));
			}
			
		}
		
		public static float ema(List<Float> observations)
		{
			if(observations.size()==1){
				 
				return observations.get(0);
			}
				
			else{
			 
				
				return SMOOTHING_FACTOR*observations.get(observations.size()-1)+(1-SMOOTHING_FACTOR)*
						ema(observations.subList(0, observations.size()-1));
			}
			
		}
		public static float mean(List<Float> observations){
			Float mean=0f;
			for (Float ob : observations) {
				mean+=ob;
			}
			return mean/(float) observations.size();
		}
		public static void main(String[] args) {
			List<Float> ob=new LinkedList<Float>();
			ob.add(0.1f);
			ob.add(0.1f);
			ob.add(0.1f);
			ob.add(0.2f);
			ob.add(0.3f);
			ob.add(0.4f);
			ob.add(0.5f);
			ob.add(0.6f);
			System.out.println(ema(ob));
 			System.out.println(mean(ob));
		}
}
