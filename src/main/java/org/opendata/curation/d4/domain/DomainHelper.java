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
import org.opendata.core.util.ArrayHelper;
import org.opendata.db.eq.EQIndex;

/**
 * Collection of helper methods for domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainHelper {
    
    private final HashMap<Integer, Integer> _domainSizes;
    private final int[] _nodeSizes;
    
    public DomainHelper(EQIndex nodes, IdentifiableObjectSet<Domain> domains) {
        
        _nodeSizes = nodes.nodeSizes();
        
        _domainSizes = new HashMap<>();
        for (Domain domain : domains) {
            int size = 0;
            for (int nodeId : domain) {
                size += _nodeSizes[nodeId];
            }
            _domainSizes.put(domain.id(), size);
        }
    }
    
    public BigDecimal termOverlap(Domain domI, Domain domJ) {
        
        int overlap = ArrayHelper.overlap(domI.nodes(), domJ.nodes(), _nodeSizes);

        int sizeI = _domainSizes.get(domI.id());
        int sizeJ = _domainSizes.get(domJ.id());
        
        return new JaccardIndex().sim(sizeI, sizeJ, overlap);
    }
}
