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
package org.opendata.curation.d4.update;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.SortedObjectSet;
import org.opendata.core.sort.IdentifiableObjectSort;
import org.opendata.core.util.IdentifiableCount;
import org.opendata.core.util.IdentifiableCounterSet;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQFileWriter;
import org.opendata.db.eq.EQWriter;

/**
 * Replace local domains with a separate equivalence class for each of them.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ReplaceLocalDomains {

    /**
     * Equivalence class for which the set of columns can be modified.
     * 
     */
    private class MutableEQ extends IdentifiableObjectImpl {

        private final HashMap<Integer, Integer> _columns;
        private final List<EQ> _eqs;

        public MutableEQ(int id, HashMap<Integer, Integer> columns, List<EQ> eqs) {

            super(id);

            _columns = columns;
            _eqs = eqs;
        }

        public MutableEQ(EQ eq) {

            super(eq.id());

            _columns = new HashMap<>();
            for (IdentifiableInteger col : eq.columnFrequencies()) {
                _columns.put(col.id(), col.value());
            }
            _eqs = new ArrayList<>();
            _eqs.add(eq);
        }

        public List<EQ> eqs() {

            return _eqs;
        }
        
        public SortedObjectSet<IdentifiableInteger> columns() {
            
            IdentifiableInteger[] columns = new IdentifiableInteger[_columns.size()];
            int index = 0;
            for (Integer columnId : _columns.keySet()) {
                columns[index++] = new IdentifiableCount(columnId, _columns.get(columnId));
            }
            Arrays.sort(columns, new IdentifiableObjectSort());
            return new SortedObjectSet<>(columns);
        }

        public int columnFrequency(int columnId) {

            return _columns.getOrDefault(columnId, 0);
        }

        public Iterable<Integer> columnIdentifiers() {
            
            return _columns.keySet();
        }

        public boolean isEmpty() {
            
            return _columns.isEmpty();
        }
        
        public String key() {

            return new HashIDSet(_columns.keySet()).toIntString();
        }
        
        public void merge(MutableEQ eq) {
        
            _eqs.addAll(eq.eqs());
            for (Integer columnId : eq.columnIdentifiers()) {
                int value = _columns.getOrDefault(columnId, 0) + eq.columnFrequency(columnId);
                _columns.put(columnId, value);
            }
        }
        
        public void removeColumn(int columnId) {

            _columns.remove(columnId);
        }
        
        public List<Integer> terms() {
            
            HashIDSet terms = new HashIDSet();
            for (EQ eq : _eqs) {
                for (Integer tid : eq.terms()) {
                    terms.add(tid);
                }
            }
            return terms.toList();
        }
    }
    
    public void run(
            CompressedTermIndex eqIndex,
            Iterable<Domain> domains,
            boolean verbose,
            EQWriter writer
    ) {
        
        HashObjectSet<MutableEQ> eqs = new HashObjectSet<>();
        
        for (EQ eq : eqIndex) {
            eqs.add(new MutableEQ(eq));
        }
        
        // Create new equivalence classes for given domains.
        int eqCounter = eqs.getMaxId() + 1;
        for (Domain domain : domains) {
            List<EQ> members = new ArrayList<>();
            IdentifiableCounterSet columns = new IdentifiableCounterSet();
            for (int eqId : domain) {
                MutableEQ eq = eqs.get(eqId);
                members.addAll(eq.eqs());
                for (int columnId : domain.columns()) {
                    columns.inc(columnId, eq.columnFrequency(columnId));
                }
            }
            if (verbose) {
                System.out.println(
                        String.format(
                                "DOMAIN %d IS EQ %d",
                                domain.id(),
                                eqCounter
                        )
                );
            }
            eqs.add(new MutableEQ(eqCounter++, columns.toMapping(), members));
        }
        
        // Remove domain members from the respective columns.
        for (Domain domain : domains) {
            for (int eqId : domain) {
                MutableEQ eq = eqs.get(eqId);
                for (int columnId : domain.columns()) {
                    eq.removeColumn(columnId);
                }
            }
        }
        
        // Merge equivalence classes with equal keys.
        HashMap<String, MutableEQ> compressedEqIndex = new HashMap<>();
        for (MutableEQ eq : eqs) {
            if (eq.isEmpty()) {
                // Ignore equivalence classes that no longer occur in any column.
                continue;
            }
            String key = eq.key();
            if (compressedEqIndex.containsKey(key)) {
                MutableEQ mergeEq = compressedEqIndex.get(key);
                if (verbose) {
                    System.out.println(String.format("MERGE %d INTO %d", eq.id(), mergeEq.id()));
                }
                mergeEq.merge(eq);
            } else {
                compressedEqIndex.put(key, eq);
            }
        }
        
        for (MutableEQ eq : compressedEqIndex.values()) {
            writer.write(eq.terms(), eq.columns());
        }
        
        if (verbose) {
            System.out.println(String.format("\nNUMBER OF EQUIVALENCE CLASSES: %d", compressedEqIndex.size()));
        }
    }

    
    public void run(
            CompressedTermIndex eqIndex,
            Iterable<Domain> domains,
            boolean verbose,
            File outputFile
    ) throws java.io.IOException {
        
        FileSystem.createParentFolder(outputFile);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            this.run(eqIndex, domains, verbose, new EQFileWriter(out));
        }
    }
}
