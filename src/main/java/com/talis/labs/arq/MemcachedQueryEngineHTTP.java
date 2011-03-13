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

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.util.Timer;

public class MemcachedQueryEngineHTTP extends QueryEngineHTTP implements Cloneable {

    private String key = null;
    private MemcachedClient client;
    public static int TTL = 60*60*24; // TTL is 1 day in seconds

    public MemcachedQueryEngineHTTP(String serviceURI, Query query) {
        super(serviceURI, query);
        this.key = Integer.toString(new Pair<String, Query>(serviceURI, query).hashCode());
        try {
            this.client = new MemcachedClient(AddrUtil.getAddresses("127.0.0.1:11211"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MemcachedQueryEngineHTTP(String serviceURI, String queryString) {
        this(serviceURI, QueryFactory.create(queryString));
    }

    @Override
    public ResultSet execSelect() {
        ResultSet rs = null;
        
        Object value = client.get(key);
        if ( value != null ) {
            rs = ResultSetFactory.fromXML((String)value);
        } else {
            rs = super.execSelect();
            client.set(key, TTL, ResultSetFormatter.asXMLString(rs));
        }
        return rs;
    }

    @Override
    public Model execConstruct() {
        Model model = ModelFactory.createDefaultModel();
        
        Object value = client.get(key);
        if ( value != null ) {
            toModel((String)value, model);
        } else {
            model = super.execConstruct();
            client.set(key, TTL, toString(model));
        }        

        return model;
    }

    @Override
    public Model execConstruct(Model m) {
        Model model = ModelFactory.createDefaultModel();
        
        Object value = client.get(key);
        if ( value != null ) {
            toModel((String)value, model);
        } else {
            model = super.execConstruct(m);
            client.set(key, TTL, toString(model));
        }        

        return model;
    }

    @Override
    public Model execDescribe() {
        Model model = ModelFactory.createDefaultModel();
        
        Object value = client.get(key);
        if ( value != null ) {
            toModel((String)value, model);
        } else {
            model = super.execDescribe();
            client.set(key, TTL, toString(model));
        }        

        return model;
    }

    @Override
    public Model execDescribe(Model m) {
        Model model = ModelFactory.createDefaultModel();
        
        Object value = client.get(key);
        if ( value != null ) {
            toModel((String)value, model);
        } else {
            model = super.execDescribe(m);
            client.set(key, TTL, toString(model));
        }

        return model;
    }

    @Override
    public boolean execAsk() {
        boolean ask;
        
        Object value = client.get(key);
        if ( value != null ) {
            ask = Boolean.parseBoolean((String)value);
        } else {
            ask = super.execAsk();
            client.set(key, TTL, Boolean.toString(ask));
        }

        return ask;
    }

    @Override
    public void close() {
        client.shutdown();
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
//            MemcachedQueryEngineHTTP qexec = new MemcachedQueryEngineHTTP(serviceURI, "SELECT * { ?s ?p ?o } LIMIT 100000");
//            MemcachedQueryEngineHTTP qexec = new MemcachedQueryEngineHTTP(serviceURI, "CONSTRUCT {?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 100");
//            try {
//                ResultSet results = qexec.execSelect();
//                for (; results.hasNext();) {
//                    results.nextSolution();
//                }
//            } finally {
//                qexec.close();
//            }

            MemcachedQueryEngineHTTP qexec = new MemcachedQueryEngineHTTP(serviceURI, "CONSTRUCT {?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 1000");
            try {
                Model model = qexec.execConstruct();
                StmtIterator iter = model.listStatements();
                while ( iter.hasNext() ) {
                    iter.next();
                }
            } finally {
                qexec.close();
            }

            
            System.out.println("query " + i + " " + timerQuery.endTimer());
            
            Thread.sleep(1000);
        }
    }

}