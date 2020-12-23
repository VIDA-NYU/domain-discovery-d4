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
import java.util.HashMap;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.set.SortedIDArray;
import org.opendata.core.set.SortedIDList;

/**
 * Similarity function for equivalence classes based on the Jaccard Index
 * similarity of their columns sets. This implementation maintains column
 * lists as sorted arrays of column identifier.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JISimilarityArray implements EQSimilarity {

    private final HashMap<Integer, SortedIDList> _equivalenceClasses;
    private final JaccardIndex _ji;
    
    public JISimilarityArray(HashMap<Integer, SortedIDList> equivalenceClasses) {
    
        _equivalenceClasses = equivalenceClasses;
        
        _ji = new JaccardIndex();
    }
    
    public JISimilarityArray() {
        
        this(new HashMap<>());
    }

    /**
     * Add an entry to the internal list of equivalence classes. Returns a
     * reference to this object.
     * 
     * @param eqId
     * @param columns
     * @return 
     */
    public JISimilarityArray add(Integer eqId, Integer[] columns) {
        
        this.consume(eqId, new SortedIDArray(columns));
        return this;
    }

    /**
     * Consumer method that adds an entry to the internal list of equivalence
     * classes.
     * 
     * @param eqId
     * @param columns
     */
    public void consume(int eqId, SortedIDList columns) {
        
        _equivalenceClasses.put(eqId, columns);
    }

    @Override
    public BigDecimal sim(int eq1, int eq2) {

        final SortedIDList cols1 = _equivalenceClasses.get(eq1);
        final SortedIDList cols2 = _equivalenceClasses.get(eq2);
        
        int overlap = cols1.overlap(cols2);
        
        return _ji.sim(cols1.length(), cols2.length(), overlap);
    }
}
