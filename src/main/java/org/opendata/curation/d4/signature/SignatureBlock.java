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
package org.opendata.curation.d4.signature;

import org.opendata.core.set.SortedIDList;

/**
 * Block in the robust signature for an equivalence class. Each block maintains
 * a list of block elements and the size of the block in the number of terms
 * for all equivalence classes in the block.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface SignatureBlock extends SortedIDList {
    
    /**
     * Total number of terms for all equivalence classes in the block.
     * 
     * @return 
     */
    public int termCount();
}
