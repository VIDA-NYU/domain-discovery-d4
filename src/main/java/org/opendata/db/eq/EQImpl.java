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
package org.opendata.db.eq;

import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.util.IdentifiableCount;
import org.opendata.core.util.StringHelper;

/**
 * Default implementation for an equivalence class. Maintain all information
 * in memory.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQImpl extends IdentifiableObjectImpl implements EQ {
    
    private final String[] _tokens;
    
    public EQImpl(String[] tokens) {
        
        super(Integer.parseInt(tokens[0]));
        
        _tokens = tokens;
    }
    
    public EQImpl(String line) {
        
        this(line.split("\t"));
    }
    
    @Override
    public Integer[] columns() {
        
        String[] tokens = _tokens[2].split(",");
        Integer[] columns = new Integer[tokens.length];
        for (int iToken = 0; iToken < tokens.length; iToken++) {
            String token = tokens[iToken];
            Integer columnId;
            if (token.contains(":")) {
                columnId = Integer.parseInt(token.substring(0, token.indexOf(":")));
            } else {
                columnId = Integer.parseInt(token);
            }
            columns[iToken] = columnId;
        }
        return columns;
    }
    
    @Override
    public int columnCount() {
        
        return StringHelper.splitSize(_tokens[2], ',');
    }

    @Override
    public IdentifiableInteger[] columnFrequencies() {

        String[] tokens = _tokens[2].split(",");
        IdentifiableInteger[] columns = new IdentifiableInteger[tokens.length];
        for (int iToken = 0; iToken < tokens.length; iToken++) {
            String token = tokens[iToken];
            int pos = token.indexOf(":");
            int columnId = Integer.parseInt(token.substring(0, pos));
            int freq = Integer.parseInt(token.substring(pos + 1));
            columns[iToken] = new IdentifiableCount(columnId, freq);
        }
        return columns;
    }
    
    @Override
    public Integer[] terms() {
        
        String[] tokens = _tokens[1].split(",");
        Integer[] terms = new Integer[tokens.length];
        for (int iToken = 0; iToken < tokens.length; iToken++) {
            terms[iToken] = Integer.parseInt(tokens[iToken]);
        }
        return terms;
    }
    
    @Override
    public int termCount() {
        
        return StringHelper.splitSize(_tokens[1], ',');
    }
}
