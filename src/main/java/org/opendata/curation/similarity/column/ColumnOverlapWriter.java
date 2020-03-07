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
package org.opendata.curation.similarity.column;

import java.io.PrintWriter;

/**
 * Writer for column pair overlap. Outputs column identifier and overlap for
 * pairs of columns that have at least one term in common.
 * 
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnOverlapWriter implements ColumnOverlapConsumer {

    private final PrintWriter _out;
    
    public ColumnOverlapWriter(PrintWriter out) {
        
        _out = out;
    }
    
    @Override
    public void consume(int colI, int colJ, int overlap) {
        
        if (overlap > 0) {
            synchronized(this) {
                _out.println(colI + "\t" + colJ + "\t" + overlap);
            }
        }
    }
}
