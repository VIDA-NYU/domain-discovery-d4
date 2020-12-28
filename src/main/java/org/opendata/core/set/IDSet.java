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

import java.math.BigDecimal;
import java.util.List;
import org.opendata.core.object.ObjectFilter;

/**
 * List of unique identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface IDSet extends ObjectSet<Integer>, ObjectFilter<Integer> {
    
    public IDSet difference(IDSet list);
    public IDSet difference(int id);
    public int first();
    public IDSet intersect(IDSet list);
    public boolean isTrueSubsetOf(IDSet list);
    public BigDecimal ji(IDSet list);
    public int maxId();
    public int minId();
    public boolean overlaps(IDSet set, int threshold);
    public boolean replace(int sourceId, int targetId);
    public IDSet sample(int size);
    public int[] toArray();
    public String toIntString();
    public List<Integer> toSortedList();
    public IDSet union(IDSet list);
    public IDSet union(int id);
}
