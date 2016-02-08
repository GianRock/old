package com.rock.twitterEventDetector.configuration;

/**
 * file contentenente configurazioni per il sistema 
 * @author Rocco
 *
 */
public class Constant {
	 
   
	/**
	    * GUI CONFIGS 
	    */
		public static final String CLUSTER_INFO_VIEW_FXML="/gui/fxml/clusterInfoView.fxml";
		public static final String TWEET_INFO_VIEW_FXML="/gui/fxml/TweetInfo2.fxml";

		/**
		    *END OF GUI CONFIGS 
		 */
 
 
	
	public final static String INDEX_DIR_LEMMATIZ="./src/main/resources/nlp/luceneIndexLemma";
	public final static String INDEX_DIR="./src/main/resources/nlp/indexTweetsEventDataset";

	public final static String PROFILE_DET_LANG_DIR="/nlp/langDetection/profiles";
	
	
	
	/**
	 * DB CONFIGGS
	 */
	public static final String MONGO_URL="127.0.0.1";
	public static final String MONGO_DB_NAME="tweetEventDataset";
	public static final String MONGO_DB_NAME_TWEETDATASET="tweetEventDataset";
	public static final String MONGO_TWEET_COLLECTION_NAME="tweets";
	 
}
