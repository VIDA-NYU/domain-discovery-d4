/*
 * This file is part of the Data-Driven Domain Discovery Tool (D4).
 * 
 * Copyright (c) 2018-2020 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opendata.curation.d4.signature;

import java.math.BigDecimal;
import java.util.ArrayList;

import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.signature.similarity.ColumnSetJaccard;
import org.opendata.curation.d4.signature.similarity.NodeSimilarityFunction;
import org.opendata.db.eq.Node;


/**
 * Generate context signatures for equivalence classes (column elements).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureGenerator {

	private final boolean _includeSelf;
    private final IdentifiableObjectSet<Node> _nodes;
    private final NodeSimilarityFunction _simFunc;
    
    /**
     * Initialize the list of nodes in the database and the similarity function
     * that is used to compute signature elements.
     * 
     * @param nodes
     * @param simFunc
     */
    public ContextSignatureGenerator(
    		IdentifiableObjectSet<Node> nodes,
    		NodeSimilarityFunction simFunc,
    		boolean includeSelf
    ) {

        _nodes = nodes;
        _simFunc = simFunc;
        _includeSelf = includeSelf;
    }

    /**
     * The default similarity metric for elements in the context signature
     * is the Jaccard Similarity between column sets.
     * 
     * @param nodes
     */
    public ContextSignatureGenerator(IdentifiableObjectSet<Node> nodes) {
    	
    	this(nodes, new ColumnSetJaccard(), false);
    }
    
    /**
     * Compute signature for element with given identifier.
     * 
     * @param id
     * @return 
     */
    public ContextSignature getSignature(int id) {
        
        Node node = _nodes.get(id);
        
        ArrayList<SignatureValue> elements = new ArrayList<>();
        if (_includeSelf) {
        	elements.add(new SignatureValue(id, BigDecimal.ONE));
        }
        for (Node nodeJ : _nodes) {
            if (node.id() != nodeJ.id()) {
            	BigDecimal sim = _simFunc.eval(node, nodeJ);
            	if (sim.compareTo(BigDecimal.ZERO) > 0) {
            		elements.add(new SignatureValue(nodeJ.id(), sim));
            	}
            }
        }
        
        return new ContextSignature(node.id(), elements);
    }
}
