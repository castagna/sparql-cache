package com.talis.labs.arq;

import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.sse.Item;
import com.hp.hpl.jena.sparql.sse.ItemTransformBase;

public class RenameVariablesItemTransform extends ItemTransformBase {

    private Map<Var, Var> varNamesMapping = new HashMap<Var, Var>();
    private int count = 0;
	
	@Override
    public Item transform(Item item, Node node) {
		if ( Var.isVar(node) ) {
			if ( ! varNamesMapping.containsKey(node) ) {
				varNamesMapping.put((Var)node, Var.alloc("v" + count++));
			}
	        return Item.createNode(varNamesMapping.get(node), item.getLine(), item.getColumn()) ;
		} else {
	        return Item.createNode(node, item.getLine(), item.getColumn()) ;			
		}
    }
    
}