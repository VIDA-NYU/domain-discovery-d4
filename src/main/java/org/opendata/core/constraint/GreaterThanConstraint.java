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
import org.opendata.core.util.FormatedBigDecimal;

/**
 * The constraint is satisfied if the given value is greater than a defined
 * threshold.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GreaterThanConstraint extends Threshold {

    private final BigDecimal _threshold;
    
    public GreaterThanConstraint(BigDecimal threshold) {
        
        _threshold = threshold;
    }
    
    public GreaterThanConstraint(double threshold) {
        
        this(new BigDecimal(threshold));
    }

    @Override
    public Threshold decreaseBy(BigDecimal value) {

        return new GreaterThanConstraint(_threshold.subtract(value));
    }
    
    @Override
    public boolean isSatisfied(BigDecimal value) {

        return (value.compareTo(_threshold) > 0);
    }

    @Override
    public String toPlainString() {
        
        return Threshold.GT + new FormatedBigDecimal(_threshold, 2);
    }
}
