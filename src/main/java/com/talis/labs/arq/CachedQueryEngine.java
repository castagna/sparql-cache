package com.talis.labs.arq;

import com.hp.hpl.jena.query.Query ;
import com.hp.hpl.jena.sparql.ARQInternalErrorException ;
import com.hp.hpl.jena.sparql.algebra.Op ;
import com.hp.hpl.jena.sparql.algebra.Transform ;
import com.hp.hpl.jena.sparql.algebra.TransformCopy ;
import com.hp.hpl.jena.sparql.algebra.Transformer ;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP ;
import com.hp.hpl.jena.sparql.core.DatasetGraph ;
import com.hp.hpl.jena.sparql.engine.Plan ;
import com.hp.hpl.jena.sparql.engine.QueryEngineFactory ;
import com.hp.hpl.jena.sparql.engine.QueryEngineRegistry ;
import com.hp.hpl.jena.sparql.engine.QueryIterator ;
import com.hp.hpl.jena.sparql.engine.binding.Binding ;
import com.hp.hpl.jena.sparql.engine.main.QueryEngineMain ;
import com.hp.hpl.jena.sparql.util.Context ;

public class CachedQueryEngine extends QueryEngineMain {

    public CachedQueryEngine(Query query, DatasetGraph dataset, Binding initial, Context context) {
        super(query, dataset, initial, context);
    }

    public CachedQueryEngine(Query query, DatasetGraph dataset) { 
        this(query, dataset, null, null);
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding initial, Context context) {
        Transform transform = new IdentityTransform();
        op = Transformer.transform(transform, op);
        return super.eval(op, dsg, initial, context);
    }
    
    @Override
    protected Op modifyOp(Op op) {
        return super.modifyOp(op) ;
    }
    
    static QueryEngineFactory factory = new CachedQueryEngineFactory();
    static public QueryEngineFactory getFactory() { return factory; } 
    static public void register() { QueryEngineRegistry.addFactory(factory); }
    static public void unregister() { QueryEngineRegistry.removeFactory(factory); }

    static class IdentityTransform extends TransformCopy {
        @Override
        public Op transform(OpBGP opBGP) { return opBGP; }
    }

    static class CachedQueryEngineFactory implements QueryEngineFactory {

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) { return true; }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding initial, Context context) {
            CachedQueryEngine engine = new CachedQueryEngine(query, dataset, initial, context) ;
            return engine.getPlan() ;
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) { return false; }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
            throw new ARQInternalErrorException("CachedQueryEngineFactory: factory called directly with an algebra expression") ;
        }

    } 

}