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
package org.opendata.core.graph.components;

import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.graph.build.GraphBuilderEdgeCondition;

/**
 * Draw an edge between two nodes if both nodes contain each other's id in their
 * id set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class MutualEdgeCondition <T extends IdentifiableIDSet> implements GraphBuilderEdgeCondition {

    private final GraphBuilderEdgeCondition _condition;
    
    public MutualEdgeCondition(GraphBuilderEdgeCondition condition) {
    
        _condition = condition;
    }
    
    @Override
    public boolean hasEdge(int sourceId, int targetId) {

        if (_condition.isSymmetric()) {
            return _condition.hasEdge(sourceId, targetId);
        } else {
            return _condition.hasEdge(sourceId, targetId) && _condition.hasEdge(targetId, sourceId);
        }
    }

    @Override
    public boolean isSymmetric() {

        return true;
    }
}
