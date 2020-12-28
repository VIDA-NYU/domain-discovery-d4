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
package org.opendata.profiling.datatype;

/**
 * Simple type annotator that only distinguishes between integer, long, decimal,
 * and text.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleDBTypeAnnotator implements DataTypeAnnotator {

    private final DataTypeChecker[] _typeCheckers;
    
    /**
     * Initialize the arrays of type checkers the default annotator uses.
     * 
     */
    public SimpleDBTypeAnnotator() {
        
        _typeCheckers = new DataTypeChecker[]{
            new DefaultIntegerChecker(),
            new LongChecker(),
            new DefaultDecimalChecker()
        };
    }
    
    @Override
    public DataType getType(String value) {

	for (DataTypeChecker tChecker : _typeCheckers) {
	    if (tChecker.isMatch(value)) {
		return tChecker.label();
	    }
	}
        return new TextType();
    }
}
