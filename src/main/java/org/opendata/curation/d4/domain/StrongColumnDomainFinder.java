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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.ImmutableIdentifiableIDSet;
import org.opendata.core.util.count.IdentifiableCounterSet;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksStream;

/**
 * Generator for local domain in columns that are highly likely to be
 * homogeneous. Looks for columns where all values in the column provide
 * strong support for each other via their conservative sigantures.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongColumnDomainFinder {

	private interface ColumnDomainFinder {
		
		public boolean isStrongDomain(HashObjectSet<ImmutableIdentifiableIDSet> nodes);
	}
	
	private class MututalSupportDomain implements ColumnDomainFinder {

		@Override
		public boolean isStrongDomain(HashObjectSet<ImmutableIdentifiableIDSet> signatures) {

			IDSet column = signatures.keys();
			
			if (column.length() == 1) {
        		return false;
        	}
        	IdentifiableCounterSet counts = new IdentifiableCounterSet();
        	for (int nodeId : column) {
        		for (int supId : signatures.get(nodeId)) {
        			if (column.contains(supId)) {
        				counts.inc(supId);
        			}
        		}
        	}
        	return counts.getMinValue() >= column.length() - 1;
		}
	}
	
	private class MututalEdgeDomain implements ColumnDomainFinder {

		@Override
		public boolean isStrongDomain(HashObjectSet<ImmutableIdentifiableIDSet> nodes) {

			IDSet column = nodes.keys();
			
			UndirectedConnectedComponents compGen;
			compGen = new UndirectedConnectedComponents(column);
        	List<ImmutableIdentifiableIDSet> nodeList = nodes.toList();
        	for (int iNode = 0; iNode < nodeList.size() - 1; iNode++) {
        		ImmutableIdentifiableIDSet nodeI = nodeList.get(iNode);
        		for (int jNode = iNode + 1; jNode < nodeList.size(); jNode++) {
        			ImmutableIdentifiableIDSet nodeJ = nodeList.get(jNode);
        			if ((nodeI.contains(nodeJ.id()) && (nodeJ.contains(nodeI.id())))) {
        				compGen.edge(nodeI.id(), nodeJ.id());
        			}
        		}
        	}
        	if (compGen.componentCount() == 1) {
        		IdentifiableIDSet domain;
        		domain = compGen.getComponents().toList().get(0);
        		if (domain.length() == column.length()) {
    				return true;
        		}
        	}
        	return false;
		}		
	}
	
	private class ConservativeSignatureCollector implements SignatureBlocksConsumer {

		private final HashMap<Integer, int[]> _signatures = new HashMap<>();
		
		@Override
		public void close() {
			
		}

		@Override
		public void consume(SignatureBlocks sig) {
			
			if (!sig.isEmpty()) {
				_signatures.put(sig.id(), sig.get(0));
			}
		}

		public int[] get(int id) {
			
			if (_signatures.containsKey(id)) {
				return _signatures.get(id);
			} else {
				return new int[0];
			}
		}
		@Override
		public void open() {
			
		}
	}
	
    public void run(
            ExpandedColumnIndex columnIndex,
            SignatureBlocksStream reader,
            String domainType,
            DomainConsumer out
    ) throws java.io.IOException {

    	ColumnDomainFinder domainFinder;
    	if (domainType.contentEquals("MUTUAL")) {
    		domainFinder = new MututalEdgeDomain();
    	} else if (domainType.contentEquals("SUPPORT")) {
    		domainFinder = new MututalSupportDomain();
    	} else {
    		throw new IllegalArgumentException("Unknown domain type: " + domainType);
    	}
    	
        System.out.println("START @ " + new Date());

        ConservativeSignatureCollector signatures;
        signatures = new ConservativeSignatureCollector();
        reader.stream(signatures);
        
        System.out.println("SIGNATURES READER @ " + new Date());
        
        // Sort column in decreasing number of nodes
        List<ExpandedColumn> columnList = new ArrayList<>(columnIndex.columns());
        Collections.sort(columnList, (ExpandedColumn c1, ExpandedColumn c2) -> 
                Integer.compare(c1.nodes().length(), c2.nodes().length())
        );
        Collections.reverse(columnList);
        
        UniqueDomainSet domainCandidates = new UniqueDomainSet(columnIndex);
        
        for (ExpandedColumn column : columnList) {
        	if (column.length() < 2) {
        		continue;
        	}
        	HashObjectSet<ImmutableIdentifiableIDSet> nodes = new HashObjectSet<>();
        	for (int nodeId : column.nodes()) {
        		nodes.add(new ImmutableIdentifiableIDSet(nodeId, signatures.get(nodeId)));
        	}
        	if (domainFinder.isStrongDomain(nodes)) {
        		domainCandidates.put(column.id(), column.nodes());
        	}
        }
        
        domainCandidates.streamNonContained(out);
        
        System.out.println("END @ " + new Date());
    }
    
    private static final String COMMAND =
            "Usage\n" +
            "  <signature-file(s)>\n" +
            "  <columns-file>\n" +
            "  <domain-type> [MUTUAL | SUPPORT]\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(StrongColumnDomainFinder.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Mutual Edge Complete Columns Finder - Version (" + Constants.VERSION + ")\n");

        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File signatureFile = new File(args[0]);
        File columnFile = new File(args[1]);
        String domainType = args[2].toUpperCase();
        File outputFile = new File(args[3]);

        FileSystem.createParentFolder(outputFile);
        
        try {
            // Read the node index and the list of columns
            // Read the list of column identifier if a columns file was given
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            new ExpandedColumnReader(columnFile).stream(columnIndex);
            new StrongColumnDomainFinder().run(
                    columnIndex,
                    new SignatureBlocksReader(signatureFile),
                    domainType,
                    new DomainWriter(outputFile)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
