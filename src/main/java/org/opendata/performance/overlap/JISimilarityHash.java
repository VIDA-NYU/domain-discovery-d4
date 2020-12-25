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
package org.opendata.performance.overlap;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import org.opendata.core.io.FileSystem;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.db.eq.similarity.EQSimilarity;

/**
 * Similarity function for equivalence classes based on the similarity of their
 * column sets. Computes the Jaccard Index similarity for the column sets of
 * two equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JISimilarityHash implements EQSimilarity {

    private final JaccardIndex _ji;
    private final HashMap<Integer, HashSet<Integer>> _nodes;
    
    public JISimilarityHash(HashMap<Integer, HashSet<Integer>> nodes) {
        
        _nodes = nodes;
        
        _ji = new JaccardIndex();
    }

    public static JISimilarityHash load(File eqFile) throws java.io.IOException {
        
        // Load column lists for each node.
        HashMap<Integer, HashSet<Integer>> nodes = new HashMap<>();
        try (BufferedReader in = FileSystem.openReader(eqFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int eqId = Integer.parseInt(tokens[0]);
                HashSet<Integer> columns = new HashSet<>();
                for (String col : tokens[2].split(",")) {
                    columns.add(Integer.parseInt(col.substring(0, col.indexOf(":"))));
                }
                nodes.put(eqId, columns);
            }
        }
        return new JISimilarityHash(nodes);
    }
    
    @Override
    public BigDecimal sim(int eq1, int eq2) {

        HashSet<Integer> nodeI = _nodes.get(eq1);
        HashSet<Integer> nodeJ = _nodes.get(eq2);
        
        if (nodeJ.size() < nodeI.size()) {
            HashSet<Integer> h = nodeJ;
            nodeJ = nodeI;
            nodeI = h;
        }
        
        int overlap = 0;
        for (int nodeId : nodeI) {
            if (nodeJ.contains(nodeId)) {
                overlap++;
            }
        }
        if (overlap > 0) {
            return _ji.sim(nodeI.size(), nodeJ.size(), overlap);
        } else {
            return BigDecimal.ZERO;
        }
    }
}
