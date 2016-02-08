package com.rock.twitterEventDetector.nlp.annotator;


 import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;


public class RestClient {
    public static final int HTTP_OK = 200;

    public static String doGet(final String url,boolean json) throws HttpException,
            IOException, URISyntaxException {



        HttpGet httpget = new HttpGet(url);
        if(json)
            httpget.setHeader("Accept", "application/json");
        final HttpClient httpClient = new DefaultHttpClient();
        HttpConnectionParams
                .setConnectionTimeout(httpClient.getParams(), 10000);
        HttpResponse response = httpClient.execute(httpget);


        String out=null;
        // System.out.println(response.getStatusLine().getStatusCode());
        if(response.getStatusLine().getStatusCode()==HTTP_OK){
            HttpEntity en=response.getEntity();


            out   = EntityUtils.toString(en,"UTF-8");
        }

        out = new String(out.getBytes("UTF-8"), "UTF-8");
        return out;
    }

    public static String doPost(final String url, final AbstractHttpEntity entity)
            throws URISyntaxException, HttpException, IOException {

        final HttpClient httpClient = new DefaultHttpClient();
        HttpConnectionParams
                .setConnectionTimeout(httpClient.getParams(), 100000);

        HttpPost httpPost = new HttpPost(url);
        // StringEntity entity = new StringEntity(POSTText, "UTF-8");
        BasicHeader basicHeader = new BasicHeader(HTTP.CONTENT_TYPE,
                "application/json");

        if(entity!=null){
            httpPost.getParams().setBooleanParameter(
                    "http.protocol.expect-continue", false);
            entity.setContentType(basicHeader);
            httpPost.setEntity(entity);
        }
        HttpResponse response = httpClient.execute(httpPost);
        String out=null;
        if(response.getStatusLine().getStatusCode()==HTTP_OK){
            HttpEntity en=response.getEntity();
            out =  EntityUtils.toString(en,"UTF-8");
        }
        System.out.println(response.getStatusLine().getStatusCode());
        if(response.getStatusLine().getStatusCode()==403)
            throw new IOException("Connection Refused");
        return out;
    }



    private static String read(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader r = new BufferedReader(new InputStreamReader(in), 1000);
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            sb.append(line);
        }
        in.close();
        return sb.toString();
    }



}