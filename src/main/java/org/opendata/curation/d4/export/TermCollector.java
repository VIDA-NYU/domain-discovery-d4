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
package org.opendata.curation.d4.export;

import org.opendata.core.set.HashIDSet;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQConsumer;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermCollector implements EQConsumer {

    private final HashIDSet _terms;
    private final int _threshold;
    
    public TermCollector(int threshold) {
        
        _threshold = threshold;
        
        _terms = new HashIDSet();
    }
    
    @Override
    public void close() {
        
    }

    @Override
    public void consume(EQ eq) {

        if (eq.terms().length() < _threshold) {
            _terms.add(eq.terms());
        } else {
            int count = 0;
            for (int termId : eq.terms()) {
                if (!_terms.contains(termId)) {
                    _terms.add(termId);
                    count++;
                    if (count >= _threshold) {
                        return;
                    }
                }
            }
        }
    }
    
    @Override
    public void open() {
        
        _terms.clear();
    }   

    public HashIDSet terms() {
        
        return _terms;
    }
}
