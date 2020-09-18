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

import java.util.ArrayList;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.metric.OverlapSimilarityFunction;
import org.opendata.db.eq.Node;


/**
 * Generate context signatures for equivalence classes (column elements).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureGenerator {

    private final OverlapSimilarityFunction _ovpFunc;
    private final IdentifiableObjectSet<Node> _nodes;
    
    public ContextSignatureGenerator(IdentifiableObjectSet<Node> nodes) {

        _nodes = nodes;
        
        _ovpFunc =  new JaccardIndex();
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
        for (Node nodeJ : _nodes) {
            if (node.id() != nodeJ.id()) {
                int overlap =  node.overlap(nodeJ);
                if (overlap > 0) {
                    double sim = _ovpFunc
                            .sim(node.columnCount(), nodeJ.columnCount(), overlap)
                            .doubleValue();
                    elements.add(new SignatureValue(nodeJ.id(), sim));
                }
            }
        }
        
        return new ContextSignature(node.id(), elements);
    }
}
