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

import java.math.BigDecimal;

import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.set.IDSet;

/**
 * Collection of common helper method for equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQHelper {

	private final int[] _nodeSizes;
	
	public EQHelper(EQIndex nodes) {
		
		this(nodes.nodeSizes());
	}
	
	public EQHelper(int[] nodeSizes) {
				
		_nodeSizes = nodeSizes;
	}
	
    public BigDecimal getJI(IDSet block1, IDSet block2) {
        
        int overlap = this.getOverlap(block1, block2);
        if (overlap > 0) {
        	return new JaccardIndex()
        			.sim(this.setSize(block1), this.setSize(block2), overlap);
        } else {
        	return BigDecimal.ZERO;
        }
    }
       
    public int getOverlap(IDSet block1, IDSet block2) {
        
        int overlap = 0;
        for (int nodeId : block2) {
            if (block1.contains(nodeId)) {
                overlap +=_nodeSizes[nodeId];
            }
        }
        return overlap;
    }
       
    public int setSize(IDSet set) {
        
        int size = 0;
        for (int nodeId : set) {
            size += _nodeSizes[nodeId];
        }
        return size;
    }
}
