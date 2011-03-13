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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.openjena.atlas.lib.ActionKeyValue;
import org.openjena.atlas.lib.Cache;
import org.openjena.atlas.lib.CacheFactory;
import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.QueryEngineFactory;
import com.hp.hpl.jena.sparql.engine.QueryEngineRegistry;
import com.hp.hpl.jena.sparql.engine.QueryExecutionBase;
import com.hp.hpl.jena.sparql.resultset.ResultSetRewindable;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.util.Timer;

public class CachedQueryEngine extends QueryExecutionBase {

    private Pair<Dataset, Query> key = null;
    private static Cache<Pair<Dataset, Query>, Object> cache = CacheFactory.createCacheUnbounded();

    static {
        cache.setDropHandler(new ActionKeyValue<Pair<Dataset, Query>, Object>() {
            @Override public void apply(Pair<Dataset, Query> key, Object value) {}
        });
    }

    public CachedQueryEngine(Query query, Dataset dataset, Context context, QueryEngineFactory qeFactory) {
        super(query, dataset, context, qeFactory);
        this.key = new Pair<Dataset, Query>(dataset, query);
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

    public static void invalidate(Dataset dataset) {
        synchronized (cache) {
            Iterator<Pair<Dataset, Query>> iter = cache.keys();
            while (iter.hasNext()) {
                Pair<Dataset, Query> key = iter.next();
                if (key.getLeft().equals(dataset)) {
                    iter.remove();
                }
            }
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        File path = new File ("src/test/resources/dataset/");
        List<String> uriList = new ArrayList<String>();
        for (File file : path.listFiles()) {
            if ( file.isFile() ) {
                uriList.add(file.getAbsolutePath());
            }
        }
        Dataset dataset = DatasetFactory.create("src/test/resources/dataset/dft.n3", uriList);
        for (int i = 0; i < 100000; i++) {
            Timer timerQuery = new Timer();
            timerQuery.startTimer();
            Query query = QueryFactory.create("SELECT * { ?s_" + random.nextInt(100) + " ?p ?o } LIMIT 100");
            Context context = ARQ.getContext().copy();
            QueryEngineFactory qeFactory = QueryEngineRegistry.get().find(query, dataset.asDatasetGraph(), context);
            CachedQueryEngine qexec = new CachedQueryEngine(query, dataset, context, qeFactory);
            if (i % 1000 == 0) {
                Timer timerInvalidate = new Timer();
                timerInvalidate.startTimer();
                CachedQueryEngine.invalidate(dataset);
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