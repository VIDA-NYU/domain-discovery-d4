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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.set.ImmutableIdentifiableIDSet;

/**
 * Default connected component generator. Use  for nodes that are unstructured
 * integers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class UndirectedConnectedComponents implements ConnectedComponentGenerator {

    private final HashMap<Integer, HashSet<Integer>> _components;
    private final Integer[] _componentMap;
    private final IDSet _nodes;
    
    public UndirectedConnectedComponents(IDSet nodes) {
	
        _nodes = nodes;
        
        _components = new HashMap<>();
        
        int maxId = nodes.maxId();
        _componentMap = new Integer[maxId + 1];
        for (int nodeId : nodes) {
            _componentMap[nodeId] = nodeId;
        }
    }

    @Override
    public void add(int nodeId, List<Integer> edges) {

        for (int target : edges) {
            this.edge(nodeId, target);
        }
    }
    
    public int componentCount() {
        
        return _components.size();
    }

    public synchronized boolean contains(int nodeId) {
    
        //return _nodes.contains(nodeId);
        if (nodeId < _componentMap.length) {
            return (_componentMap[nodeId] != null);
        } else {
            return false;
        }
    }
    
    public synchronized void edge(int sourceId, int targetId) {	
        
        int sourceCompId = _componentMap[sourceId];
        int targetCompId = _componentMap[targetId];

        if (sourceCompId != targetCompId) {
            // The respective components may not have been instantiated yet.
            boolean sourceExists = _components.containsKey(sourceCompId);
            boolean targetExists = _components.containsKey(targetCompId);
            if ((sourceExists) && (targetExists)) {
                // Merge the two components. We add the values from the smaller
                // component to the larger one
                HashSet<Integer> source = _components.get(sourceCompId);
                HashSet<Integer> target = _components.get(targetCompId);
                if (source.size() > target.size()) {
                    this.merge(source, sourceCompId, target, targetCompId);
                } else {
                    this.merge(target, targetCompId, source, sourceCompId);
                }
            } else if ((sourceExists) && (!targetExists)) {
                // Add targetId to source component
                _components.get(sourceCompId).add(targetId);
                _componentMap[targetId] = sourceCompId;
            } else if ((!sourceExists) && (targetExists)) {
                // Add sourceId to target component
                _components.get(targetCompId).add(sourceId);
                _componentMap[sourceId] = targetCompId;
            } else {
                // Create component for source and add target
                HashSet<Integer> comp = new HashSet<>();
                comp.add(sourceId);
                comp.add(targetId);
                _components.put(sourceCompId, comp);
                _componentMap[targetId] = sourceCompId;
            }
        }
    }
    
    @Override
    public synchronized IdentifiableObjectSet<IdentifiableIDSet> getComponents() {

        HashObjectSet<IdentifiableIDSet> result = new HashObjectSet<>();
	
        HashIDSet clusteredNodes = new HashIDSet();
        
        for (int compId : _components.keySet()) {
            HashSet<Integer> comp = _components.get(compId);
            ImmutableIDSet cluster = new ImmutableIDSet(comp);
            result.add(new ImmutableIdentifiableIDSet(compId, cluster));
            clusteredNodes.add(cluster);
	}
        
        // Add single node components for all unclustered nodes
        for (int nodeId : _nodes) {
            if (!clusteredNodes.contains(nodeId)) {
                result.add(
                        new ImmutableIdentifiableIDSet(
                                nodeId,
                                new ImmutableIDSet(nodeId)
                        )
                );
            }
        }
	
        return result;
    }
    
    /**
     * Test if all nodes belong to the same single component.
     * 
     * @return 
     */
    public boolean isComplete() {
       
        if (_components.size() == 1) {
            int compSize = _components.values().iterator().next().size();
            return (compSize == _nodes.length());
        }
        return false;
    }

    private void merge(
            HashSet<Integer> target,
            int targetCompId,
            HashSet<Integer> source,
            int sourceCompId
    ) {
        
        for (int nodeId : source) {
            target.add(nodeId);
            _componentMap[nodeId] = targetCompId;
        }
        
        _components.remove(sourceCompId);
    }
}
