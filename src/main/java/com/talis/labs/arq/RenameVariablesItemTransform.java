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

import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.sse.Item;
import com.hp.hpl.jena.sparql.sse.ItemTransformBase;

public class RenameVariablesItemTransform extends ItemTransformBase {

    private Map<Var, Var> varsMapping = new HashMap<Var, Var>();
    private int count = 0;

    @Override
    public Item transform(Item item, Node node) {
        if (Var.isVar(node)) {
            if (!varsMapping.containsKey(node)) {
                varsMapping.put((Var) node, Var.alloc("v" + count++));
            }
            return Item.createNode(varsMapping.get(node), item.getLine(), item.getColumn());
        } else {
            return Item.createNode(node, item.getLine(), item.getColumn());
        }
    }

}