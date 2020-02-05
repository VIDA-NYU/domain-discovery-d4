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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.graph.build.GraphBuilderEdgeCondition;
import org.opendata.core.graph.build.GraphBuilderTask;

/**
 * Merge nodes in a graph in parallel to generate connected components.
 * 
 * Merging is based in a given edge condition for graph nodes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ComponentGenerator <T extends IdentifiableObject> {
   
    private final GraphBuilderEdgeCondition _condition;
    private final ConnectedComponentGenerator _compGen;
    private final List<T> _nodes;

    public ComponentGenerator(
            List<T> nodes,
            GraphBuilderEdgeCondition condition,
            ConnectedComponentGenerator compGen
    ) {
        _nodes = nodes;
        _condition = condition;
        _compGen = compGen;
    }
    
    public ConnectedComponentGenerator run(int threads) throws java.lang.InterruptedException {

	System.out.println("START COMPONENT GENERATOR RUN @ " + new java.util.Date());
	
        if (threads == 1) {
            new GraphBuilderTask(_nodes, _compGen, _condition).run();
        } else {
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < threads; iThread++) {
                es.execute(
                        new GraphBuilderTask(
                                iThread,
                                threads,
                                _nodes,
                                _compGen,
                                _condition
                        )
                );
            }
            es.shutdown();
            es.awaitTermination(1, TimeUnit.DAYS);
        }
        
	System.out.println("END COMPONENT GENERATOR RUN @ " + new java.util.Date());

	return _compGen;
    }
    
    public ConnectedComponentGenerator run() {
        
        new GraphBuilderTask(_nodes, _compGen, _condition).run();
        return _compGen;
    }
}
