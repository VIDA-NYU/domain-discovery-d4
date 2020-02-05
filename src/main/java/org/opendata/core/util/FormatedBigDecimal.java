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
package org.opendata.core.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Get a formated string of a BigDecimal at specified scale.
 * 
 * This class is primarily used for printing floating point numbers in a
 * formated way.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class FormatedBigDecimal {
   
    private final int _scale;
    private final BigDecimal _value;
    
    public FormatedBigDecimal(BigDecimal value, int scale) {
        
        _value = value;
        _scale = scale;
    }
    
    public FormatedBigDecimal(BigDecimal value) {
        
        this(value, 8);
    }
    
    public FormatedBigDecimal(double value, int scale) {
        
        this(new BigDecimal(value), scale);
    }
    
    public FormatedBigDecimal(double value) {
        
        this(new BigDecimal(value), 8);
    }
    
    public boolean isGreaterThan(BigDecimal value) {
        
        return _value.compareTo(value) > 0;
    }
    
    public boolean isGreaterOrEqual(BigDecimal value) {
        
        return _value.compareTo(value) >= 0;
    }
    
    public boolean isLowerThan(BigDecimal value) {
        
        return _value.compareTo(value) < 0;
    }
    
    public boolean isLowerOrEqual(BigDecimal value) {
        
        return _value.compareTo(value) <= 0;
    }
    
    @Override
    public String toString() {
        
        return _value.setScale(_scale, RoundingMode.HALF_DOWN).toPlainString();
    }
    
    public BigDecimal value() {
        
        return _value;
    }
}
