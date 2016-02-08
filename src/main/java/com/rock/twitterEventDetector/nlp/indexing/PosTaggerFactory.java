package com.rock.twitterEventDetector.nlp.indexing;

import com.rock.twitterFlashMobDetector.nlp.annotator.semanticAnnotators.dbpedia.DbpediaAnnotator;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.Serializable;

public class PosTaggerFactory implements PooledObjectFactory<MyPosTagger>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7690116455844023555L;
	@Override
	public void activateObject(PooledObject<MyPosTagger> arg0) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destroyObject(PooledObject<MyPosTagger> arg0) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PooledObject<MyPosTagger> makeObject() throws Exception {
		// TODO Auto-generated method stub
		return new DefaultPooledObject<MyPosTagger>(new MyPosTagger());
	}

	@Override
	public void passivateObject(PooledObject<MyPosTagger> arg0) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean validateObject(PooledObject<MyPosTagger> pooledObj) {
		// TODO Auto-generated method stub
		
		return true;
	}
	public static void main(String[] args) {
		GenericObjectPool<MyPosTagger> pool=new GenericObjectPool<MyPosTagger>(new PosTaggerFactory());
		pool.setMaxIdle(40);
		try {
			MyPosTagger posTagger=pool.borrowObject();
			DbpediaAnnotator d=new DbpediaAnnotator();
			//d.annotateAllTweets2(tweets);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
