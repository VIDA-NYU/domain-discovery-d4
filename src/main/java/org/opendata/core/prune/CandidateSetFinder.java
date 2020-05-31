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
package org.opendata.core.prune;

import java.util.ArrayList;
import java.util.List;
import org.opendata.core.object.IdentifiableDecimal;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.ImmutableIDSet;

/**
 * For a given list of identifiable double, find the pruning index for an
 * implementation-specific pruning condition.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public abstract class CandidateSetFinder <T extends IdentifiableDecimal> {
   
    /**
     * Return the pruning index.
     * 
     * @param elements
     * @return 
     */
    public int getPruneIndex(List<T> elements) {
        
        return this.getPruneIndex(elements, 0);
    }
    
    /**
     * Return pruning index after the given start position.
     * 
     * @param elements
     * @param start
     * @return 
     */
    public abstract int getPruneIndex(List<T> elements, int start);
    
    /**
     * Return identifier of elements that occur before the pruning index.
     * 
     * @param elements
     * @return 
     */
    public IDSet pruneElements(List<T> elements) {
        
        int pruneIndex = this.getPruneIndex(elements);
        if (pruneIndex > 0) {
            List<Integer> result = new ArrayList<>();
            for (int iIndex = 0; iIndex < pruneIndex; iIndex++) {
                result.add(elements.get(iIndex).id());
            }
            return new ImmutableIDSet(result);
        } else {
            return new ImmutableIDSet();
        }
    }    
}
