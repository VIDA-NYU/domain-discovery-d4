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
import org.opendata.core.object.IdentifiableDouble;

/**
 * Similarity function for equivalence classes based on a weighted Jaccard
 * similarity. The algorithm is based on:
 * 
 * https://mathoverflow.net/questions/123339/weighted-jaccard-similarity
 * http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/36928.pdf
 * 
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class WeightedJISimilarity implements EQSimilarity {

    private final IdentifiableDouble[][] _nodes;
    
    public WeightedJISimilarity(IdentifiableDouble[][] nodes) {
        
        _nodes = nodes;
    }
    
    @Override
    public SimilarityScore sim(int eq1, int eq2) {

        final IdentifiableDouble[] nodeI = _nodes[eq1];
        final IdentifiableDouble[] nodeJ = _nodes[eq2];
        
        final int len1 = nodeI.length;
        final int len2 = nodeJ.length;
        
        int idx1 = 0;
        int idx2 = 0;

        double divident = 0;
        double divisor = 0;
        int overlap = 0;
        
        while ((idx1 < len1) && (idx2 < len2)) {
            IdentifiableDouble nI = nodeI[idx1];
            IdentifiableDouble nJ = nodeJ[idx2];
            int comp = Integer.compare(nI.id(), nJ.id());
            if (comp < 0) {
                divisor += nI.value();
                idx1++;
            } else if (comp > 0) {
                divisor += nJ.value();
                idx2++;
            } else {
                divident += Math.min(nI.value(), nJ.value());
                divisor += Math.max(nI.value(), nJ.value());
                overlap++;
                idx1++;
                idx2++;
            }
        }

        while (idx1 < len1) {
            divisor += nodeI[idx1++].value();
        }
        while (idx2 < len2) {
            divisor += nodeJ[idx2++].value();
        }
        
        if (overlap > 0) {
            return new SimilarityScore(overlap, new BigDecimal(divident / divisor));
        } else {
            return new SimilarityScore(0, BigDecimal.ZERO);
        }
    }
}
