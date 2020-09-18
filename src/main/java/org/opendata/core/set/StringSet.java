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
package org.opendata.core.set;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.io.FileSystem;

/**
 * Set of strings.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StringSet extends ObjectSetImpl<String> implements ObjectSet<String> {
   
    private final HashSet<String> _elements;

    public StringSet() {
        
        _elements = new HashSet<>();
    }
    
    public StringSet(File file, boolean toTerm) throws java.io.IOException {
    
        this();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                if (toTerm) {
                    line = line.trim().toUpperCase();
                }
                if (!line.equals("")) {
                    _elements.add(line);
                }
            }
        }
    }

    public StringSet(File file) throws java.io.IOException {
        
        this(file, false);
    }
    
    public StringSet(String[] values) {
	
        this();

        for (String value : values) {
            _elements.add(value);
        }
    }
    
    public StringSet(String value) {
        
        this();
        _elements.add(value);
    }
    
    public void add(String value) {
        
        _elements.add(value);
    }
    
    public void add(ObjectSet<String> values) {
        
        for (String value : values) {
            _elements.add(value);
        }
    }
    
    @Override
    public boolean contains(String element) {

        return _elements.contains(element);
    }

    public StringSet intersect(StringSet list) {

	StringSet innerList;
	StringSet outerList;
	
        if (this.length() > list.length()) {
	    outerList = list;
	    innerList = this;
	} else {
	    outerList = this;
	    innerList = list;
	}
	
        StringSet values = new StringSet();
        for (String value : outerList) {
            if (innerList.contains(value)) {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public boolean isEmpty() {

        return _elements.isEmpty();
    }

    @Override
    public Iterator<String> iterator() {

        return _elements.iterator();
    }

    @Override
    public int length() {

        return _elements.size();
    }

    public void remove(String value) {
        
        _elements.remove(value);
    }
    
    @Override
    public List<String> toList() {

        return new ArrayList<>(_elements);
    }
    
    public List<String> toSortedList() {

        List<String> result = new ArrayList<>(_elements);
        Collections.sort(result);
        return result;
    }
    
    public void write(File file) throws java.io.IOException {
        
        List<String> elements = this.toList();
        Collections.sort(elements);
        
        try (PrintWriter out = FileSystem.openPrintWriter(file)) {
            for (String value : elements) {
                out.println(value);
            }
        }
    }
 }
