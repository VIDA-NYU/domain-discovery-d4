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
package org.opendata.core.prune;

import java.util.List;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.object.IdentifiableDouble;

/**
 * Find steepest drop in list of identifiable double values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class MaxDropFinder <T extends IdentifiableDouble> extends CandidateSetFinder<T> {
        
    private final Threshold _nonEmptySignatureThreshold;
    private final boolean _ignoreLastDrop;
    private final boolean _fullSignatureConstraint;
    
    public MaxDropFinder(
            Threshold nonEmptySignatureThreshold,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop
    ) {
        _nonEmptySignatureThreshold = nonEmptySignatureThreshold;
        _fullSignatureConstraint = fullSignatureConstraint;
        _ignoreLastDrop = ignoreLastDrop;
    }
    
    public MaxDropFinder(
            double nonEmptySignatureThreshold,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop
    ) {
        this(
                new GreaterThanConstraint(nonEmptySignatureThreshold),
                fullSignatureConstraint,
                ignoreLastDrop
        );
    }

    /**
     * Get index position of steepest drop in a list of double values.
     * 
     * Assumes that the list is sorted in decreasing order. Returns the index
     * position of the element on the right side of the steepest drop.
     * 
     * If the list is empty the result is 0. If the first element is smaller
     * than the empty constraint threshold the result is 0. If the full
     * signature constraint is satisfied the result is the size of the element
     * vector.
     * 
     * If the list contains a single element the result is 1 or 0 (in case the
     * empty signature constraint is satisfied).
     * 
     * @param elements
     * @param start
     * @return 
     */
    @Override
    public int getPruneIndex(List<T> elements, int start) {
             
        final int size = elements.size();

        // Result is zero if element list is empty.
        if (start >= size) {
            return 0;
        }
        
        // Result is zero if first element is smaller than the empty signature
        // constraint threshold.
        final double first = elements.get(0).value();
        if (!_nonEmptySignatureThreshold.isSatisfied(first)) {
            return 0;
        }
        
        // Return 1 if the size of the list is one
        if ((size - start) == 1) {
            return start + 1;
        }
        
        // If the full signature constraint is satisfied the result equals the
        // size of the array
        final double last = elements.get(size - 1).value();
        if (_fullSignatureConstraint) {
            if ((elements.get(start).value() - last) < last) {
                return size;
             }
        }
        
        double maxDiff = 0f;
        int maxIndex = size;        
        for (int iIndex = start; iIndex < size - 1; iIndex++) {
            double diff = elements.get(iIndex).value() - elements.get(iIndex + 1).value();
            if (diff > maxDiff) {
                maxIndex = iIndex + 1;
                maxDiff = diff;
            }
        }
        
        // If we do not ignore the last drop we need to check if that drop is
        // greater than the largest drop that was found.
        if ((!_ignoreLastDrop) && (last > maxDiff)) {
            maxIndex = size;
        }
        return maxIndex;
    }
}
