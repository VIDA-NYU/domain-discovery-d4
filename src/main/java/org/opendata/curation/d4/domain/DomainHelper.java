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
package org.opendata.curation.d4.domain;

import java.math.BigDecimal;
import java.util.HashMap;

import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Collection of helper methods for domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainHelper {
    
    private final HashMap<Integer, Integer> _domainSizes;
    private final Integer[] _eqTermCounts;
    
    public DomainHelper(Integer[] eqTermCounts, IdentifiableObjectSet<Domain> domains) {
        
        _eqTermCounts = eqTermCounts;
        
        _domainSizes = new HashMap<>();
        for (Domain domain : domains) {
            int size = 0;
            for (int nodeId : domain) {
                size += _eqTermCounts[nodeId];
            }
            _domainSizes.put(domain.id(), size);
        }
    }
    
    private int overlap(int[] list1, int[] list2, Integer[] nodeSizes) {
        
        final int len1 = list1.length;
        final int len2 = list2.length;
        
        int idx1 = 0;
        int idx2 = 0;
        int overlap = 0;
        
        while ((idx1 < len1) && (idx2 < len2)) {
            int comp = Integer.compare(list1[idx1], list2[idx2]);
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                overlap += nodeSizes[list1[idx1]];
                idx1++;
                idx2++;
            }
        }
        return overlap;
    }

    public BigDecimal termOverlap(Domain domI, Domain domJ) {
        
        int overlap = this.overlap(domI.nodes(), domJ.nodes(), _eqTermCounts);

        int sizeI = _domainSizes.get(domI.id());
        int sizeJ = _domainSizes.get(domJ.id());
        
        return new JaccardIndex().sim(sizeI, sizeJ, overlap);
    }
}
