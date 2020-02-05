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
package org.opendata.curation.d4;

import java.io.File;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import org.opendata.core.constraint.Threshold;

/**
 * Helper class to parse and access optional command line arguments.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CLP {
   
    private final HashMap<String, String> _elements;
    private final HashSet<String> _keys;

    public CLP(Parameter[] parameters, String[] args) {
    
        // Print help if the only argument is --help
        if (args.length == 2) {
            if (args[1].equals("--help")) {
                System.out.println(args[0]);
                for (Parameter para : parameters) {
                    System.out.println("  --" + para);
                }
                System.exit(0);
            }
        }
        _elements = new HashMap<>();
        _keys = new HashSet<>();
        
        for (Parameter para : parameters) {
            _keys.add(para.name());
        }
        
        int iArg = 1;
        while (iArg < args.length) {
            String arg = args[iArg];
            if (!arg.startsWith("--")) {
                break;
            }
            int pos = arg.indexOf("=");
            if (pos < 0) {
                System.out.println("Invalid argument: " + arg);
                System.exit(-1);
            }
            String key = arg.substring(2, pos);
            String value = arg.substring(pos + 1);
            if (!_keys.contains(key)) {
                System.out.println("Unknown argument: " + arg);
                System.exit(-1);
            }
            _elements.put(key, value);
            iArg++;
        }
        
        if (iArg < args.length) {
            String arg = args[iArg];
            System.out.println("Unknown argument: " + arg);
            System.exit(-1);
        }
    }
    
    public String get(String key) {
        
        return _elements.get(key);
    }
    
    public BigDecimal getAsBigDecimal(String key) {
        
        return new BigDecimal(this.get(key));
    }
    
    public BigDecimal getAsBigDecimal(String key, BigDecimal defaultValue) {
        
        if (_elements.containsKey(key)) {
            return this.getAsBigDecimal(key);
        } else {
            return defaultValue;
        }
    }
    
    public boolean getAsBool(String key) {
        
        return Boolean.parseBoolean(this.get(key));
    }
    
    public boolean getAsBool(String key, boolean defaultValue) {
        
        if (_elements.containsKey(key)) {
            return this.getAsBool(key);
        } else {
            return defaultValue;
        }
    }
    
    public Threshold getAsConstraint(String key, String defaultValue) {
        
        if (_elements.containsKey(key)) {
            return Threshold.getConstraint(this.get(key));
        } else {
            return Threshold.getConstraint(defaultValue);
        }
    }
    
    public File getAsFile(String key, String defaultValue) {
        
        if (_elements.containsKey(key)) {
            return new File(this.get(key));
        } else {
            return new File(defaultValue);
        }
    }
    
    public int getAsInt(String key) {
        
        return Integer.parseInt(this.get(key));
    }
    
    public int getAsInt(String key, int defaultValue) {
        
        if (_elements.containsKey(key)) {
            return this.getAsInt(key);
        } else {
            return defaultValue;
        }
    }
    
    public String getAsString(String key, String defaultValue) {
        
        if (_elements.containsKey(key)) {
            return this.get(key);
        } else {
            return defaultValue;
        }
    }
    
    public boolean has(String key) {
        
        return _elements.containsKey(key);
    }
}
