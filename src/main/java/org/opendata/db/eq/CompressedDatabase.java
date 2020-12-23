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

import java.util.HashMap;
import java.util.List;
import org.opendata.core.set.SortedIDList;

/**
 * Database that maintains a set of equivalence classes. Equivalence classes
 * are sets of terms. They represent a compressed version of the full database
 * with terms that occur in the same set of columns being represented by a
 * single equivalence class.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class CompressedDatabase {
    
    /**
     * Get mapping of equivalence class identifier to sorted list of identifier
     * for columns that the equivalence class occurs in.
     * 
     * @return 
     */
    public abstract HashMap<Integer, SortedIDList> getEQColumns();
    
    /**
     * Get list containing all unique identifiers for the equivalence classes
     * in the database.
     * 
     * @return 
     */
    public abstract List<Integer> getEQIdentifiers();

    /**
     * Get array that maps the identifier of an equivalence class to the number
     * of terms in that equivalence class.
     * 
     * @return 
     */
    public abstract Integer[] getEQTermCounts();
}
