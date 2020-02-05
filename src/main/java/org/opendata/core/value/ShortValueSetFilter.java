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

/**
 * Accepts a set of values if at least one value is shorter or equal in length
 * to a given threshold.
 * 
 * The intension is to filter out columns where all values are longer than a
 * given threshold.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ShortValueSetFilter implements ValueSetFilter {

    private final ValueSetFilter _filter;
    private final Threshold _fractionThreshold;
    private final int _maxValueLength;
    
    public ShortValueSetFilter(
            int maxValueLength,
            Threshold fractionThreshold,
            ValueSetFilter filter
    ) {
        _maxValueLength = maxValueLength;
        _fractionThreshold = fractionThreshold;
        _filter = filter;
    }

    public ShortValueSetFilter(
            int maxValueLength,
            Threshold fractionThreshold
    ) {
        this(maxValueLength, fractionThreshold, null);
    }
    
    public ShortValueSetFilter(
            int maxValueLength,
            double fractionThreshold,
            ValueSetFilter filter
    ) {
        this(maxValueLength,
                Threshold.getGreaterConstraint(fractionThreshold),
                filter
        );
    }

    public ShortValueSetFilter(
            int maxValueLength,
            double fractionThreshold
    ) {
        this(maxValueLength,
                Threshold.getGreaterConstraint(fractionThreshold),
                null
        );
    }
    
    @Override
    public boolean accept(Iterable<ValueCounter> values) {

        int count = 0;
        int size = 0;
        for (ValueCounter value : values) {
            String text = value.getText();
            if (text.length() <= _maxValueLength) {
                count++;
            }
            size++;
        }
        double frac = (double)count / (double)size;
        if (_fractionThreshold.isSatisfied(frac)) {
            if (_filter != null) {
                return _filter.accept(values);
            } else {
                return true;
            }
        }
        return false;
    }
    
}
