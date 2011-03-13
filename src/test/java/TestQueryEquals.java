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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;

import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.sse.Item;
import com.hp.hpl.jena.sparql.sse.ItemTransformer;
import com.hp.hpl.jena.sparql.sse.SSE;
import com.talis.labs.arq.MyNodeIsomorphismMap;
import com.talis.labs.arq.RenameVariablesItemTransform;

public class TestQueryEquals {

	@Test
	public void testQueryEquals1() {
		// parsing two queries from files
		Query q1 = QueryFactory.read("src/test/resources/query-1.rq");
		Query q2 = QueryFactory.read("src/test/resources/query-2.rq");
		assertEquals(q1, q2);
		assertEquals(q1.hashCode(), q2.hashCode());

		// getting their algebra
		Op op1 = Algebra.compile(q1) ;
		Op op2 = Algebra.compile(q2) ;
		assertEquals(op1, op2);
		assertEquals(op1.hashCode(), op2.hashCode());

		// ... and back to query strings
		Query qb1 = OpAsQuery.asQuery(op1) ;
		Query qb2 = OpAsQuery.asQuery(op2) ;
		assertEquals(qb1, qb2);
		assertEquals(qb1.serialize(), qb2.serialize());
		assertEquals(qb1.serialize().hashCode(), qb2.serialize().hashCode());

		// constructing the algebra programmatically
        String BASE = "http://example/" ; 
        BasicPattern bp = new BasicPattern() ;
        Var var_v = Var.alloc("v") ;
        Var var_x = Var.alloc("x") ;
        bp.add(new Triple(var_x, Node.createURI(BASE+"p"), var_v)) ;
        Op op3 = new OpBGP(bp) ;
        Expr expr = new E_Equals(new ExprVar(var_v), NodeValue.makeNodeInteger(1)) ;
        op3 = OpFilter.filter(expr, op3) ;
        ArrayList<Var> vars = new ArrayList<Var>();
        vars.add(var_x);
        vars.add(var_v);
        op3 = new OpProject(op3, vars);
		assertEquals(op1, op3);
		assertEquals(op2, op3);
		assertEquals(op1.hashCode(), op3.hashCode());
		assertEquals(op2.hashCode(), op3.hashCode());
	}
	
	@Test
	public void testQueryEquals2() {
		Query q1 = QueryFactory.read("src/test/resources/query-1.rq");
		Query q3 = QueryFactory.read("src/test/resources/query-3.rq"); // the only difference is the variable name!
		assertFalse(q1.equals(q3));
		
		Op op1 = Algebra.compile(q1);
		Op op3 = Algebra.compile(q3);
		
		assertFalse(q1.equals(q3));

		Item it1 = SSE.parse(op1.toString());
		it1 = ItemTransformer.transform(new RenameVariablesItemTransform(), it1); 
		Item it3 = SSE.parse(op3.toString());
		it3 = ItemTransformer.transform(new RenameVariablesItemTransform(), it3); 
		
		op1 = Algebra.parse(it1);
		op3 = Algebra.parse(it3);
		
		assertEquals(op1, op3);
	}
	
	@Test
	public void testQueryEquals3() {
		Query q1 = QueryFactory.read("src/test/resources/query-1.rq");
		Query q3 = QueryFactory.read("src/test/resources/query-3.rq"); // the only difference is the variable name!
		assertFalse(q1.equals(q3));
		
		Op op1 = Algebra.compile(q1);
		Op op3 = Algebra.compile(q3);
		assertFalse(q1.equals(q3));
		assertFalse(op1.equalTo(op3, new MyNodeIsomorphismMap()));
	}
	
}
