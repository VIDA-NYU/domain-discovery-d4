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

import java.io.PrintWriter;
import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.set.IDSet;

/**
 * Each equivalence class is a set of terms that always occur together in the
 * same set of columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface EQ extends IdentifiableObject {
   
    /**
     * List of identifier for columns the equivalence class occurs in.
     * 
     * @return 
     */
    public IDSet columns();
    
    /**
     * List of identifier for terms in the equivalence class.
     * 
     * @return 
     */
    public IDSet terms();
    
    /**
     * Print string representation of the equivalence class.
     * 
     * @param out 
     */
    public void write(PrintWriter out);
}
