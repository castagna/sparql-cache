/*
 * Copyright © 2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.labs.arq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Set;

import org.openjena.atlas.lib.Closeable;
import org.openjena.atlas.lib.Pair;

import redis.clients.jedis.Jedis;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.util.Timer;

public class RedisQueryEngineHTTP extends QueryEngineHTTP implements Closeable {

    private String key = null;
    private String service = null;
    private Jedis client;

    public RedisQueryEngineHTTP(String serviceURI, Query query) {
        super(serviceURI, query);
        this.key = Integer.toString(new Pair<String, Query>(serviceURI, query).hashCode());
        this.service = Integer.toString(serviceURI.hashCode());
        this.client = new Jedis("127.0.0.1");
    }

    public RedisQueryEngineHTTP(String serviceURI, String queryString) {
        this(serviceURI, QueryFactory.create(queryString));
    }

    @Override
    public ResultSet execSelect() {
        ResultSetRewindable rs = null;
        
        String value = client.get(key);
        if ( value != null ) {
            rs = ResultSetFactory.makeRewindable(ResultSetFactory.fromXML(value));
        } else {
            rs = ResultSetFactory.makeRewindable(super.execSelect());
            set(key, ResultSetFormatter.asXMLString(rs));
        }
        rs.reset();
        return rs;
    }

    @Override
    public Model execConstruct() {
        Model model = ModelFactory.createDefaultModel();
        
        String value = client.get(key);
        if ( value != null ) {
            toModel(value, model);
        } else {
            model = super.execConstruct();
            set(key, toString(model));
        }        

        return model;
    }

    @Override
    public Model execConstruct(Model m) {
        Model model = ModelFactory.createDefaultModel();
        
        String value = client.get(key);
        if ( value != null ) {
            toModel(value, model);
        } else {
            model = super.execConstruct(m);
            set(key, toString(model));
        }        

        return model;
    }

    @Override
    public Model execDescribe() {
        Model model = ModelFactory.createDefaultModel();
        
        String value = client.get(key);
        if ( value != null ) {
            toModel(value, model);
        } else {
            model = super.execDescribe();
            set(key, toString(model));
        }        

        return model;
    }

    @Override
    public Model execDescribe(Model m) {
        Model model = ModelFactory.createDefaultModel();
        
        String value = client.get(key);
        if ( value != null ) {
            toModel(value, model);
        } else {
            model = super.execDescribe(m);
            set(key, toString(model));
        }

        return model;
    }

    @Override
    public boolean execAsk() {
        boolean ask;
        
        String value = client.get(key);
        if ( value != null ) {
            ask = Boolean.parseBoolean(value);
        } else {
            ask = super.execAsk();
            set(key, Boolean.toString(ask));
        }

        return ask;
    }

    public void invalidate(String service) {
        Set<String> keys = client.smembers(Integer.toString(service.hashCode()));
        for (String key : keys) {
            client.del(key);            
        }
    }

    @Override
    public void close() {
        try {
            client.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    private void set (String key, String value) {
        client.set(key, value);
        client.sadd(service, key);
    }
    
    private void toModel(String value, Model model) {
        try {
            InputStream in = new ByteArrayInputStream(value.getBytes("UTF-8"));
            model.read(in, null, "N-TRIPLES");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
    
    private String toString(Model model) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            model.write(out, "N-TRIPLES", null);
            return new String(out.toByteArray(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static void main(String[] args) throws Exception {
        String serviceURI = "http://api.talis.com/stores/bbc-wildlife/services/sparql";
        for (int i = 0; i < 10000; i++) {
            Timer timerQuery = new Timer();
            timerQuery.startTimer();
            RedisQueryEngineHTTP qexec = new RedisQueryEngineHTTP(serviceURI, "SELECT * { ?s ?p ?o } LIMIT 1000");
            if (i % 1000 == 0) {
                Timer timerInvalidate = new Timer();
                timerInvalidate.startTimer();
                qexec.invalidate(serviceURI);
                System.out.println("cache invalidated " + timerInvalidate.endTimer());
            }            
            try {
                ResultSet results = qexec.execSelect();
                for (; results.hasNext();) {
                    results.nextSolution();
                }
            } finally {
                qexec.close();
            }
            
            System.out.println("query " + i + " " + timerQuery.endTimer());
            
            Thread.sleep(1000);
        }
    }

}