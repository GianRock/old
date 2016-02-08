package com.rock.twitterEventDetector.nlp.indexing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RepeatedPatterns
{
 	public static final Pattern REPEATED_REGEX= Pattern.compile("([a-zA-Z]+)\\1{2,}");

    public static void main(String[] args) {
        String s = "ballare ammazzzare loooooooooooooooool ??????????";
        find(s);
        String s1 = "hahahahahahahahaha .... ,,,, 3000 1999";
        find(s1);
        System.err.println("---");
        String s2 = "RonRonRon BobRonJoe";
        find(s2);
        
         /*
        List<MyTweet> tweets=ts.findAllTweetsFromMongo();
        for (MyTweet myTweet : tweets) {
        	Matcher m=REPEATED_REGEX.matcher(myTweet.getCleanedText());
    		if(m.find()){
    			System.err.println(myTweet.getCleanedText()); 
    			String newCleanedText=m.replaceAll(" ");
    			System.out.println(newCleanedText); 
    		}
		} */
    }

    private static void find(String s) {
        Matcher m = Pattern.compile("([a-zA-Z]+)\\1{2,}").matcher(s);
         while (m.find()) {
        	 for (int i = 0; i <= m.groupCount(); i++)
        	    {
        	      System.out.println(m.group(i));
        	    }
            
        }
    }
}