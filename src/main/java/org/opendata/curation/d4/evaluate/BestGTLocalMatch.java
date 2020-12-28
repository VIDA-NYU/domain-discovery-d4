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
package org.opendata.curation.d4.evaluate;

import java.util.HashMap;
import org.opendata.core.metric.F1;
import org.opendata.core.metric.Precision;
import org.opendata.core.metric.Recall;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.EQ;

/**
 * Best match for ground truth domains against a set of local domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class BestGTLocalMatch {
    
    private final int _sizeThreshold;
    
    public BestGTLocalMatch(int sizeThreshold) {
        
        _sizeThreshold = sizeThreshold;
    }
    
    public HashMap<String, BestMatch> run(
            CompressedTermIndex eqIndex,
            HashMap<String, IDSet> groundTruths,
            IdentifiableObjectSet<Domain> domains
    ) {
        
        HashMap<String, BestMatch> bestMatches = new HashMap<>();
        for (String key : groundTruths.keySet()) {
            bestMatches.put(key, new BestMatch());
        }
        
        HashMap<Integer, Integer[]> termIndex = new HashMap<>();
        for (EQ eq : eqIndex) {
            termIndex.put(eq.id(), eq.terms());
        }
        
        for (Domain domain : domains) {
            HashIDSet terms = new HashIDSet();
            boolean ignore = false;
            for (int nodeId : domain) {
                Integer[] eqTerms = termIndex.get(nodeId);
                if ((terms.length() + eqTerms.length) > _sizeThreshold) {
                    ignore = true;
                    break;
                }
                for (Integer termId : eqTerms) {
                    terms.add(termId);
                }
            }
            if (ignore) {
                continue;
            }
            for (String key : groundTruths.keySet()) {
                IDSet gt = groundTruths.get(key);
                int ovp = terms.overlap(gt);
                if (ovp > 0) {
                    Precision precision = new Precision(ovp, terms.length());
                    Recall recall = new Recall(ovp, gt.length());
                    F1 match = new F1(precision, recall);
                    BestMatch gtBestMatch = bestMatches.get(key);
                    if (gtBestMatch.f1().compareTo(match) < 0) {
                        bestMatches.put(key, new BestMatch(domain.id(), precision, recall));
                    }
                }
            }
        }

        return bestMatches;
    }
}
