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
package org.opendata.db.column;

import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Element in an aggregated column.
 * 
 * This can either be a single term or an equivalence class of terms. Maintains
 * the list of columns the element occurs in and the frequency of the element in
 * each column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public interface ColumnElement <T extends IdentifiableObject> extends IdentifiableObject, Comparable<ColumnElement> {
    
    /**
     * Number of columns the element occurs in.
     * 
     * This is a short-cut for the length of the columns set.
     * 
     * @return 
     */
    public int columnCount();
    
    /**
     * Set of column identifier for the columns the element occurs in.
     * 
     * @return 
     */
    public abstract IdentifiableObjectSet<T> columns();
}
