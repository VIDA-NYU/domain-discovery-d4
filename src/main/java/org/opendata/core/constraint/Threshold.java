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
package org.opendata.core.constraint;

import java.math.BigDecimal;

/**
 * Threshold constraints check whether a given value satisfies a threshold.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class Threshold {

    public static final String GEQ = "GEQ";
    public static final String GT = "GT";
    
    /**
     * Get same threshold constraint with constraint value decreased by the
     * given value.
     * 
     * @param value
     * @return 
     */
    public abstract Threshold decreaseBy(BigDecimal value);
    
    public static Threshold getConstraint(String spec) {
    
        try {
            if (spec.toUpperCase().startsWith(GEQ)) {
                return new GreaterOrEqualConstraint(
                        new BigDecimal(spec.substring(GEQ.length()))
                );
            } else if (spec.toUpperCase().startsWith(GT)) {
                return new GreaterThanConstraint(
                        new BigDecimal(spec.substring(GT.length()))
                );
            } else {
                return new GreaterThanConstraint(new BigDecimal(spec));
            }
        } catch (java.lang.NumberFormatException ex) {
            throw new java.lang.IllegalArgumentException("Invalid constraint specification: " + ex);
        }
    }
    
    public static Threshold getGreaterConstraint(BigDecimal threshold) {
        
        if (threshold.compareTo(BigDecimal.ONE) == 0) {
            return new EqualsOneConstraint();
        } else {
            return new GreaterThanConstraint(threshold);
        }
    }

    public static Threshold getGreaterConstraint(double threshold) {
        
        return getGreaterConstraint(new BigDecimal(threshold));
    }
    
    public boolean isSatisfied(double value) {
        
        return this.isSatisfied(new BigDecimal(value));
    }
    
    public abstract boolean isSatisfied(BigDecimal value);
    
    public abstract String toPlainString();
}
