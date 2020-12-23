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
package org.opendata.db.eq.similarity;

import java.math.BigDecimal;
import org.opendata.core.graph.Node;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Similarity function for equivalence classes based on the similarity of their
 * column sets. Computes the logarithm of the Jaccard Index similarity for the
 * column sets of two equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LogJISimilarity implements EQSimilarity {

    private final JaccardIndex _ji;
    private final IdentifiableObjectSet<Node> _nodes;
    
    public LogJISimilarity(IdentifiableObjectSet<Node> nodes) {
        
        _nodes = nodes;
        
        _ji = new JaccardIndex();
    }
    
    @Override
    public BigDecimal sim(int eq1, int eq2) {

        Node nodeI = _nodes.get(eq1);
        Node nodeJ = _nodes.get(eq2);
        
        int overlap = nodeI.overlap(nodeJ);
        if (overlap > 0) {
            return _ji.logSim(nodeI.elementCount(), nodeJ.elementCount(), overlap);
        } else {
            return BigDecimal.ZERO;
        }
    }
}
