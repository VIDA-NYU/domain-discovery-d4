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
package org.opendata.db.eq;

import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Collection of common helper method for equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQHelper {
   
    public static <T extends EQ> int setSize(
            IDSet set,
            IdentifiableObjectSet<T> nodes
    ) {
        
        int size = 0;
        for (int nodeId : set) {
            size += nodes.get(nodeId).termCount();
        }
        return size;
    }

    public static <T extends EQ> int setSize(
            int[] set,
            int[] nodes
    ) {
        
        int size = 0;
        for (int nodeId : set) {
            size += nodes[nodeId];
        }
        return size;
    }

    public static <T extends EQ> IDSet setTerms(
            IDSet set,
            IdentifiableObjectSet<T> nodes
    ) {
        
        HashIDSet terms = new HashIDSet();
        for (int nodeId : set) {
            terms.add(nodes.get(nodeId).terms());
        }
        return terms;
    }

    public static <T extends EQ> int getOverlap(
            IDSet block1,
            IDSet block2,
            IdentifiableObjectSet<T> nodes
    ) {
        
        int overlap = 0;
        for (int nodeId : block2) {
            if (block1.contains(nodeId)) {
                overlap += nodes.get(nodeId).termCount();
            }
        }
        return overlap;
    }
    
}
