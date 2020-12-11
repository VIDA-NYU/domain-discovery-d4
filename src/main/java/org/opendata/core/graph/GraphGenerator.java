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
package org.opendata.core.graph;

/**
 * Simple interface for dynamic graph generators. The generators implement a
 * single method that allows to add an edge between a pair of nodes. Whether
 * that edge is considered a directed or a un-directed edge is implementation
 * dependent.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface GraphGenerator {
   
    /**
     * Add a new edge between a pair of nodes to the graph.
     * 
     * @param source
     * @param target 
     */
    public void edge(int source, int target);
}
