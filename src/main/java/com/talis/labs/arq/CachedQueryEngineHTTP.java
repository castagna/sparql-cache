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

import java.util.Iterator;
import java.util.Random;

import org.openjena.atlas.lib.ActionKeyValue;
import org.openjena.atlas.lib.Cache;
import org.openjena.atlas.lib.CacheFactory;
import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.resultset.ResultSetRewindable;
import com.hp.hpl.jena.sparql.util.Timer;

public class CachedQueryEngineHTTP extends QueryEngineHTTP {

    private Pair<String, Query> key = null;
    private static Cache<Pair<String, Query>, Object> cache = CacheFactory.createCacheUnbounded();

    static {
        cache.setDropHandler(new ActionKeyValue<Pair<String, Query>, Object>() {
            @Override public void apply(Pair<String, Query> key, Object value) {}
        });
    }

    public CachedQueryEngineHTTP(String serviceURI, Query query) {
        super(serviceURI, query);
        this.key = new Pair<String, Query>(serviceURI, query);
    }

    public CachedQueryEngineHTTP(String serviceURI, String queryString) {
        this(serviceURI, QueryFactory.create(queryString));
    }

    @Override
    public ResultSet execSelect() {
        ResultSetRewindable rs = null;
        synchronized (cache) {
            if (cache.containsKey(key)) {
                return (ResultSetRewindable) cache.get(key);
            }
        }

        rs = ResultSetFactory.makeRewindable(super.execSelect());
        synchronized (cache) {
            cache.put(key, rs);
        }
        rs.reset();

        return rs;
    }

    @Override
    public Model execConstruct() {
        Model model = ModelFactory.createDefaultModel();
        synchronized (cache) {
            if (cache.containsKey(key)) {
                return model.add((Model) cache.get(key));
            }
        }

        model = super.execConstruct();
        synchronized (cache) {
            cache.put(key, model);
        }

        return model;
    }

    @Override
    public Model execConstruct(Model m) {
        Model model = ModelFactory.createDefaultModel();
        synchronized (cache) {
            if (cache.containsKey(key)) {
                return model.add((Model) cache.get(key));
            }
        }

        model = super.execConstruct(m);
        synchronized (cache) {
            cache.put(key, model);
        }

        return model;
    }

    @Override
    public Model execDescribe() {
        Model model = ModelFactory.createDefaultModel();
        synchronized (cache) {
            if (cache.containsKey(key)) {
                model.add((Model) cache.get(key));
            }
        }

        model = super.execDescribe();
        synchronized (cache) {
            cache.put(key, model);
        }

        return model;
    }

    @Override
    public Model execDescribe(Model m) {
        Model model = ModelFactory.createDefaultModel();
        synchronized (cache) {
            if (cache.containsKey(key)) {
                model.add((Model) cache.get(key));
            }
        }

        model = super.execDescribe(m);
        synchronized (cache) {
            cache.put(key, model);
        }

        return model;
    }

    @Override
    public boolean execAsk() {
        boolean ask;
        synchronized (cache) {
            if (cache.containsKey(key)) {
                ask = (Boolean) cache.get(key);
            }
        }

        ask = super.execAsk();
        synchronized (cache) {
            cache.put(key, ask);
        }

        return ask;
    }

    public static void invalidate(String serviceURI) {
        synchronized (cache) {
            Iterator<Pair<String, Query>> iter = cache.keys();
            while (iter.hasNext()) {
                Pair<String, Query> key = iter.next();
                if (key.getLeft().equals(serviceURI)) {
                    iter.remove();
                }
            }
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        String serviceURI = "http://api.talis.com/stores/bbc-wildlife/services/sparql";
        for (int i = 0; i < 100000; i++) {
            Timer timerQuery = new Timer();
            timerQuery.startTimer();
            CachedQueryEngineHTTP qexec = new CachedQueryEngineHTTP(serviceURI,
                    "SELECT * { ?s_" + random.nextInt(100) + " ?p ?o } LIMIT 100");
            if (i % 1000 == 0) {
                Timer timerInvalidate = new Timer();
                timerInvalidate.startTimer();
                CachedQueryEngineHTTP.invalidate(serviceURI);
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
        }
    }

}