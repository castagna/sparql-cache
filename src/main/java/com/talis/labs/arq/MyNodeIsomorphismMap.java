package com.talis.labs.arq;

import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

public class MyNodeIsomorphismMap extends NodeIsomorphismMap {

    private Map<Node, Node> map = new HashMap<Node, Node>() ;
    private Node get(Node key) { return map.get(key) ; }
    private void put(Node key, Node value) { map.put(key, value) ; }
	
	@Override
    public boolean makeIsomorhpic(Node n1, Node n2) {
        if ( n1.isBlank() && n2.isBlank() ) {
            Node other = get(n1) ;
            if ( other == null ) {
                put(n1, n2) ;
                return true ;
            }
            return other.equals(n2) ;
        } else if ( n1.isVariable() && n2.isVariable() ) {
            Node other = get(n1) ;
            if ( other == null ) {
                put(n1, n2) ;
                return true ;
            }
            return other.equals(n2) ;        	
        }
        return n1.equals(n2) ;
    }
	
	
}