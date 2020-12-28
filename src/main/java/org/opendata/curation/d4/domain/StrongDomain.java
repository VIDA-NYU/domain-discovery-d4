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
package org.opendata.curation.d4.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.DoubleValueDescSort;

/**
 * A strong domain is a set of local domains. Each member node in the strong
 * domain has a weight that reflects the fraction of contained local domains
 * the node is a member of. The list of columns is the union of columns that
 * all local domains occur in.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongDomain extends IdentifiableObjectImpl {
    
    private final IDSet _columns;
    private final IDSet _localDomains;
    private final IdentifiableObjectSet<StrongDomainMember> _members;
    
    public StrongDomain(
            int id,
            IdentifiableObjectSet<StrongDomainMember> members,
            IDSet localDomains,
            IDSet columns
    ) {
        super(id);
        
        _localDomains = localDomains;
        _members = members;
        _columns = columns;
    }
    
    /**
     * Set of column identifier. This set contains identifier for all columns
     * at least one of the local domains in this strong domain occurs in.
     * 
     * @return 
     */
    public IDSet columns() {
        
        return _columns;
    }
    /**
     * Get blocks of term identifier for strong domain members. Each term is
     * assigned a weight based on the number of local domains it occurs in and
     * blocks are generated using steepest drop.
     * 
     * @param eqIndex
     * @return 
     */
    public List<List<IdentifiableDouble>> getBlocksWithWeights() {
        
        List<IdentifiableDouble> nodes = new ArrayList<>();
        for (StrongDomainMember node : this.members()) {
            double weight = node.weight().doubleValue();
            nodes.add(new IdentifiableDouble(node.id(), weight));
            Collections.sort(nodes, new DoubleValueDescSort<>());
        }

        MaxDropFinder<IdentifiableDouble> dropFinder;
        dropFinder = new MaxDropFinder<>(0.0, true, true);

        ArrayList<List<IdentifiableDouble>> blocks = new ArrayList<>();

        int start = 0;
        final int end = nodes.size();        
        while (start < end) {
            int pruneIndex = dropFinder.getPruneIndex(nodes, start);
            if (pruneIndex <= start) {
                break;
            }
            List<IdentifiableDouble> block = new ArrayList<>();
            for (int iEl = start; iEl < pruneIndex; iEl++) {
                block.add(nodes.get(iEl));
            }
            blocks.add(block);
            start = pruneIndex;
        }
        return blocks;
    }
    
    /**
     * Set of identifier for local domains that compose this strong domain.
     * 
     * @return 
     */
    public IDSet localDomains() {
        
        return _localDomains;
    }
    
    /**
     * List of nodes that are included in the strong domain. Each member belongs
     * to at least one of the local domain. The member weight is the fraction of
     * local domains in this strong domain that the node is contained in.
     * @return 
     */
    public IdentifiableObjectSet<StrongDomainMember> members() {
        
        return _members;
    }
}
