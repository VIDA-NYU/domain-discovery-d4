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
package org.opendata.curation.d4.column;

import org.opendata.core.set.MutableIdentifiableIDSet;

/**
 * Set of supporting nodes for a column node during column expansion. This class
 * is used to be able to output the support the individual nodes have during
 * expansion.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SupportSet extends MutableIdentifiableIDSet {
    
    private boolean _added = false;

    public SupportSet(int id) {
        
        super(id);
    }
    
    public void added() {
        
        _added = true;
    }
    
    public boolean wasAdded() {
        
        return _added;
    }
}
