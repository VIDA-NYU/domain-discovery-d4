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
import org.opendata.core.object.ObjectFilter;

/**
 * Generic set of comparable objects.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public interface ObjectSet <T> extends Iterable<T>, ObjectFilter<T> {
    
    public boolean contains(ObjectSet<T> values);
    public boolean isEmpty();
    public boolean isTrueSubsetOf(ObjectSet<T> list);
    public int length();
    public int overlap(ObjectSet<T> list);
    public boolean overlaps(ObjectSet<T> list);
    public boolean sameSetAs(ObjectSet<T> list);
    public List<T> toList();
}
