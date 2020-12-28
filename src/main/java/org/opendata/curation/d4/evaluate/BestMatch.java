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
package org.opendata.curation.d4.evaluate;

import org.opendata.core.metric.F1;
import org.opendata.core.metric.Precision;
import org.opendata.core.metric.Recall;

/**
 * Information about a best match with a ground truth domain.
 * 
 * @author heiko
 */
public class BestMatch {
    
    private final int _domainId;
    private final F1 _f1;
    private final Precision _precision;
    private final Recall _recall;
    
    public BestMatch(int domainId, Precision precision, Recall recall) {
        
        _domainId = domainId;
        _precision = precision;
        _recall = recall;
        _f1 = new F1(precision, recall);
;
    }
    
    public BestMatch() {
        
        this(-1, new Precision(), new Recall());
    }
    
    public int domainId() {
        
        return _domainId;
    }
    
    public F1 f1() {
        
        return _f1;
    }
    
    public Precision precision() {
        
        return _precision;
    }
    
    public Recall recall() {
        
        return _recall;
    }
}
