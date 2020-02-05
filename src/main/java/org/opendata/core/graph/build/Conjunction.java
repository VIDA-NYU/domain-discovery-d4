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
package org.opendata.core.graph.build;

import java.util.ArrayList;
import java.util.List;

/**
 * Evaluate whether a pair of domains can be merged using a conjunction of
 * merge conditions.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Conjunction implements GraphBuilderEdgeCondition {
   
    private final List<GraphBuilderEdgeCondition> _conditions;

    public Conjunction(List<GraphBuilderEdgeCondition> conditions) {
    
        _conditions = conditions;
    }

    public Conjunction() {
        
        this(new ArrayList<GraphBuilderEdgeCondition>());
    }
    
    public Conjunction add(GraphBuilderEdgeCondition condition) {
        
        _conditions.add(condition);
        
        return this;
    }
    
    @Override
    public boolean hasEdge(int sourceId, int targetId) {

        for (GraphBuilderEdgeCondition cond : _conditions) {
            if (!cond.hasEdge(sourceId, targetId)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isSymmetric() {

        for (GraphBuilderEdgeCondition cond : _conditions) {
            if (!cond.isSymmetric()) {
                return false;
            }
        }
        return true;
    }
}
