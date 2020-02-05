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

import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IDSetConsumer;
import org.opendata.core.set.ImmutableIDSet;

/**
 * Clique finder using the Born-Kerbosch algorithm.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BornKerbosch {
    
    private final IDSetConsumer _consumer;
    
    public BornKerbosch(IDSetConsumer consumer) {
        
        _consumer = consumer;
    }
    
    public void findCliques(AdjacencyGraph graph, int v) {
        
        IDSet p = graph.nodes();
        IDSet r = new ImmutableIDSet();
        IDSet x = new ImmutableIDSet();
        
        IDSet neighbors = new HashIDSet(graph.adjacent(v));
        this.findCliquesPivot(
            graph,
            r.union(v),
            p.intersect(neighbors),
            x.intersect(neighbors)
        );
    }

    private void findCliquesPivot(AdjacencyGraph graph, IDSet r, IDSet p, IDSet x) {

        if ((p.isEmpty()) && (x.isEmpty())) {
            _consumer.consume(r);
        } else {
            int u = p.union(x).first();
            for (int v : p.difference(new HashIDSet(graph.adjacent(u)))) {
                IDSet neighbors = new HashIDSet(graph.adjacent(v));
                this.findCliquesPivot(
                    graph,
                    r.union(v),
                    p.intersect(neighbors),
                    x.intersect(neighbors)
                );
                r = r.difference(v);
                x = x.union(v);
            }
        }
    }
    
}
