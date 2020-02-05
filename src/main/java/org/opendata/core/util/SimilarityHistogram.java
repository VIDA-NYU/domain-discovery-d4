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

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.util.count.LongCounter;

/**
 * Create a histogram from a stream of similarity values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimilarityHistogram {
    
    private final HashMap<String, LongCounter> _histogram;
    private final int _scale;
    private long _totalSize = 0;
    
    public SimilarityHistogram(int scale) {
        
        _scale = scale;
        
        _histogram = new HashMap<>();
        for (int iBucket = 0; iBucket < Math.pow(10, _scale) + 1; iBucket++) {
            String key = String.format("%0" + _scale + "d", iBucket);
            if (key.length() == _scale) {
                key = "0." + key;
                _histogram.put(key, new LongCounter(0));
            }
        }
	String key = "1." + String.format("%0" + _scale + "d", 0);
	_histogram.put(key, new LongCounter(0));
    }
    
    public SimilarityHistogram() {
        
        this(2);
    }
    
    public void add(BigDecimal val, int count) {
        
        String key = val
                .setScale(8, RoundingMode.CEILING)
                .toPlainString()
                .substring(0, _scale + 2);
        
        synchronized(this) {
            try {
                _histogram.get(key).inc();
            } catch (java.lang.NullPointerException ex) {
                _histogram.put(key, new LongCounter(1));
            }
            _totalSize++;
        }
    }

    public void add(BigDecimal val) {
        
        this.add(val, 1);
    }
    
    public void add(double val, int count) {
        
        this.add(new BigDecimal(val), count);
    }
    
    public void add(double val) {
        
        this.add(new BigDecimal(val));
    }
    
    public List<Bin> bins() {

        ArrayList<Bin> bins = new ArrayList<>();
        
        for (int iBucket = 0; iBucket < Math.pow(10, _scale) + 1; iBucket++) {
            String key = String.format("%0" + _scale + "d", iBucket);
            if (key.length() == _scale) {
                key = "0." + key;
            }
            long val = this.get(key);
            bins.add(
                            new Bin(
                            (double)(iBucket) / Math.pow(10, _scale),
                            (double)(iBucket + 1) / Math.pow(10, _scale),
                            new BigDecimal(val)
                                    .divide(new BigDecimal(_totalSize), MathContext.DECIMAL64)
                                    .doubleValue(),
                            val
                    )
            );
        }
        
        return bins;
    }
    
    public HashMap<String, LongCounter> buckets() {
        
        return _histogram;
    }
    
    public long get(String key) {
        
        if (_histogram.containsKey(key)) {
            return _histogram.get(key).value();
        } else {
            return 0;
        }
    }
    
    public List<String> keys() {
        
        List<String> keys = new ArrayList<>();
        
        for (int iBucket = 0; iBucket < Math.pow(10, _scale) + 1; iBucket++) {
           keys.add("0." + String.format("%0" + _scale + "d", iBucket));
        }

        return keys;
    }
    
    private void print(String key, long count) {
	
	BigDecimal frac = new BigDecimal(count)
		.divide(new BigDecimal(_totalSize), MathContext.DECIMAL64)
		.setScale(8, RoundingMode.HALF_DOWN);
	System.out.println(key + "\t" + count + "\t" + frac.toPlainString());
    }
    
    public long totalSize() {
        
        return _totalSize;
    }
    
    public void write() {
        
        for (int iBucket = 0; iBucket < Math.pow(10, _scale) + 1; iBucket++) {
            String key = String.format("%0" + _scale + "d", iBucket);
            if (key.length() == _scale) {
                key = "0." + key;
		this.print(key, _histogram.get(key).value());
            }
        }
	String key = "1." + String.format("%0" + _scale + "d", 0);
	this.print(key, _histogram.get(key).value());
        System.out.println("SUM\t" + _totalSize);
    }
    
    public void write(PrintWriter out) {
        
        for (int iBucket = 0; iBucket < Math.pow(10, _scale) + 1; iBucket++) {
            String key = String.format("%0" + _scale + "d", iBucket);
            if (key.length() == _scale) {
                key = "0." + key;
                out.println(key + "\t" + _histogram.get(key).value());
            }
        }
	String key = "1." + String.format("%0" + _scale + "d", 0);
	out.println(key + "\t" + _histogram.get(key).value());
    }
}
