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

import java.io.PrintWriter;
import org.apache.commons.text.StringEscapeUtils;
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermFileWriter implements TermConsumer {

    private final ObjectFilter _filter;
    private int _maxLength = 0;
    private final PrintWriter _outTerm;
    private final PrintWriter _outColumntermMap;
    private int _trunc_count = 0;
    private final int _valueLengthThreshold;
    
    public TermFileWriter(
            ObjectFilter filter,
            int valueLengthThreshold,
            PrintWriter outTerm,
            PrintWriter outColumntermMap
    ) {
	_filter = filter;
        _valueLengthThreshold = valueLengthThreshold;
        _outTerm = outTerm;
        _outColumntermMap = outColumntermMap;
    }

    public TermFileWriter(
            int valueLengthThreshold,
            PrintWriter outTerm,
            PrintWriter outColumntermMap
    ) {
	
	this(new AnyObjectFilter(), valueLengthThreshold, outTerm, outColumntermMap);
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(Term term) {

        if (!_filter.contains(term.id())) {
	    return;
	}
        
        for (int columnId : term.columns()) {
            _outColumntermMap.println(
                    columnId + "\t" +
                    term.id()
            );
        }
	
        String value = term.name();
        
        value = StringEscapeUtils.escapeEcmaScript(value);
        if (value.contains("\\")) {
            value = value.replaceAll("\\\\", "\\\\\\\\");
        }
        if (value.length() > _valueLengthThreshold) {
            value = value.substring(0, _valueLengthThreshold) + "__" + (_trunc_count++);
        }
        if (value.length() > _maxLength) {
            _maxLength = value.length();
        }
        _outTerm.println(term.id() + "\t" + value);
    }

    public int maxLength() {
        
        return _maxLength;
    }
    
    @Override
    public void open() {

        _trunc_count = 0;
    }
}
