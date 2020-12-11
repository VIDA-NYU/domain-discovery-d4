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

/**
 * Steepest drop information. Contains the right boundary of the drop and
 * the difference. Also contains a flag indicating whether the drop is due
 * to the full signature constraint.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Drop {
   
    private final double _diff;
    private final int _index;
    private final boolean _isFullSignature;
    
    public Drop(int index, double diff, boolean isFullSignature) {
        
        _index = index;
        _diff = diff;
        _isFullSignature = isFullSignature;
    }
    
    public Drop() {
        
        this(0, 0., false);
    }
    
    public double diff() {
        
        return _diff;
    }
    
    public int index() {
        
        return _index;
    }
    
    public boolean isFullSignature() {
        
        return _isFullSignature;
    }
}
