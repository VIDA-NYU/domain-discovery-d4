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
import java.text.DecimalFormat;

/**
 * Histogram over a set of double values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Histogram {
   
    public static final int DEFAULT_BUCKETS = 10;
    public static final double DEFAULT_MAX_VALUE = 1;
    public static final double DEFAULT_MIN_VALUE = 0;
    
    private final int _buckets;
    private int _count = 0;
    private final Bucket[] _histogram;
    private final double _minValue;
    private int _maxBucketSize = 0;
    private final double _maxValue;
    
    public Histogram(double minValue, double maxValue, int buckets) {
    
        _minValue = minValue;
        _maxValue = maxValue;
        _buckets = buckets;
        
        _histogram = new Bucket[_buckets];
        DecimalFormat df = new DecimalFormat("0.00");
        for (int iBucket = 0; iBucket < _histogram.length; iBucket++) {
            String label = df.format((double)(iBucket + 1) / (double)_buckets);
            _histogram[iBucket] = new Bucket(label);
        }
    }
    
    public Histogram(int buckets) {
        
        this(DEFAULT_MIN_VALUE, DEFAULT_MAX_VALUE, buckets);
    }
    
    public Histogram() {
        
        this(DEFAULT_MIN_VALUE, DEFAULT_MAX_VALUE, DEFAULT_BUCKETS);
    }
    
    /**
     * Add given value to the histogram.
     * 
     * @param value 
     */
    public void add(double value) {
	
        int index = Math.min(
            _buckets - 1,
            new BigDecimal(
                ((double)_buckets * value) / (_maxValue - _minValue)
            ).intValue()
        );
	int bucketSize = _histogram[index].inc();
	if (bucketSize > _maxBucketSize) {
	    _maxBucketSize = bucketSize;
	}
	_count++;
    }
    
    /**
     * Get bucket at given index position.
     * 
     * @param index
     * @return 
     */
    public Bucket get(int index) {
	
	return _histogram[index];
    }
    
    /**
     * Number of elements in the largest bucket.
     * 
     * @return 
     */
    public int getMaximumBucketSize() {
	
	return _maxBucketSize;
    }
    
    /**
     * Number of buckets in the histogram.
     * 
     * @return 
     */
    public int size() {
	
	return _histogram.length;
    }
    
    /**
     * Total number of elements that have been added to the histogram.
     * 
     * @return 
     */
    public int totalValueCount() {
        
        return _count;
    }
}
