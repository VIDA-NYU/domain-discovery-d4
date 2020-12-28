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
package org.opendata.core.value;

import org.opendata.core.constraint.Threshold;
import org.opendata.profiling.datatype.DefaultDataTypeAnnotator;

/**
 * Value set filter for text sets.
 * 
 * Accepts value sets that contain a fraction of text values which is above a
 * given threshold.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextValueSetFilter implements ValueSetFilter {

    private final Threshold _threshold;
    
    public TextValueSetFilter(Threshold threshold) {
        
        _threshold = threshold;
    }
    
    public TextValueSetFilter(double threshold) {
        
        this(Threshold.getGreaterConstraint(threshold));
    }
    
    @Override
    public boolean accept(Iterable<ValueCounter> values) {

        DefaultDataTypeAnnotator typeCheck = new DefaultDataTypeAnnotator();
        
        int textCount = 0;
        int totalCount = 0;
        for (ValueCounter val : values) {
            if (typeCheck.getType(val.getText()).isText()) {
                textCount++;
            }
            totalCount++;
        }
        
        if (textCount > 0) {
            double sup = (double)textCount/(double)totalCount;
            if (_threshold.isSatisfied(sup)) {
                return true;
            }
        }
        return false;
    }
}
