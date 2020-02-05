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
package org.opendata.core.set;

import java.util.List;

/**
 * Merge sets of IDSets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IDSetMerger {
    
    public int length(List<ImmutableIDSet> elements) {
        
        if (elements.isEmpty()) {
            return 0;
        } else if (elements.size() == 1) {
            return elements.get(0).length();
        } else {
            MultiSetIterator iter = new MultiSetIterator(elements);
            int length = 0;
            while (iter.hasNext()) {
                length++;
                iter.next();
            }
            return length;
        }
    }
    
    public ImmutableIDSet merge(List<ImmutableIDSet> elements) {
        
        if (elements.isEmpty()) {
            return new ImmutableIDSet();
        } else if (elements.size() == 1) {
            return elements.get(0);
        } else {
            return new ImmutableIDSet(new MultiSetIterator(elements));
        }
    }
}
