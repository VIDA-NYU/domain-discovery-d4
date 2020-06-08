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
package org.opendata.curation.profiling;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.EntitySetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.core.util.count.IdentifiableCounterSet;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQHelper;
import org.opendata.db.eq.EQIndex;

/**
 * Write terms that are considered NULL value candidates in clean columns
 * to file. A term (equivalence class) is considered a NULL candidate if
 * it does not belong to any domain in a clean column.
 * 
 * For each NULL candidate we output:
 * 
 * - number of clean columns where the term is a NULL candidate,
 * - number of columns where the term is in no domain,
 * - number of columns where the term is in a local domain,
 * - number of columns where the term is in a strong domain,
 * - maximum similarity value from the term signature,
 * - number of blocks in the term signature,
 * - number of terms in the term signature.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class NullValueCandidateWriter {

	private class SignatureStats extends IdentifiableObjectImpl {
		
		private final int _blockCount;
		private final BigDecimal _maxSim;
		private final int _nodeCount;
		
		public SignatureStats(SignatureBlocks sig) {
			
			super(sig.id());
			
			_blockCount = sig.size();
			_maxSim = sig.maxSim();
			_nodeCount = sig.nodeCount();
		}
		
		public int blockCount() {
			
			return _blockCount;
		}
		
		public BigDecimal maxSim() {
			
			return _maxSim;
		}
		
		public int nodeCount() {
			
			return _nodeCount;
		}
	}
	
	private class SignatureStatsConsumer implements SignatureBlocksConsumer {

		private final HashObjectSet<SignatureStats> _elements = new HashObjectSet<>();
		
		@Override
		public void close() {
			
		}

		@Override
		public void consume(SignatureBlocks sig) {
			
			_elements.add(new SignatureStats(sig));
		}

		public SignatureStats get(int id) {
			
			return _elements.get(id);
		}
		
		@Override
		public void open() {
			
		}
	}
	
	private class StatsSort implements Comparator<int[]> {

		private final SignatureStatsConsumer _signatures;
		
		public StatsSort(SignatureStatsConsumer signatures) {
			
			_signatures = signatures;
		}
		
		@Override
		public int compare(int[] arg0, int[] arg1) {
			
			return this.score(arg0).compareTo(this.score(arg1));
		}
		
		private BigDecimal score(int[] arg) {
			
			return _signatures.get(arg[4])
					.maxSim()
					.divide(new BigDecimal(arg[0]), MathContext.DECIMAL64);
		}
	}
	
	private final boolean _strict;
	
	public NullValueCandidateWriter(boolean strict) {
		
		_strict = strict;
	}
	
	public void run(
			File eqFile,
			File termFile,
			File signatureFile,
			File columnFile,
			File localDomainFile,
			File strongDomainFile,
			File outputFile
	) throws java.io.IOException {
		
        ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
        new ExpandedColumnReader(columnFile).stream(columnIndex);            

        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
			this.run(
					new EQIndex(eqFile),
					new EntitySetReader(termFile),
					new SignatureBlocksReader(signatureFile),
					columnIndex,
					new DomainReader(localDomainFile).read(),
					new StrongDomainReader(strongDomainFile).read(),
					3,
					out
			);
		}
	}
	
	public void run(
			EQIndex eqIndex,
			EntitySetReader termIndexReader,
			SignatureBlocksStream signatures,
			ExpandedColumnIndex columnIndex,
			IdentifiableObjectSet<Domain> localDomains,
			IdentifiableObjectSet<StrongDomain> strongDomains,
			int n,
			PrintWriter out
	) throws java.io.IOException {
		
        // For each column compute the set of nodes that belong to strong domains
		// or only to local domains.
        HashObjectSet<MutableIdentifiableIDSet> locDomNodes = new HashObjectSet<>();
        HashObjectSet<MutableIdentifiableIDSet> strongDomNodes = new HashObjectSet<>();
        for (Domain localDomain : localDomains) {
            HashObjectSet<MutableIdentifiableIDSet> columns = null;
            for (StrongDomain strongDomain : strongDomains) {
                if (strongDomain.localDomains().contains(localDomain.id())) {
                    columns = strongDomNodes;
                    break;
                }
            }
            if (columns == null) {
                columns = locDomNodes;
            }
            for (int columnId : localDomain.columns()) {
                if (columns.contains(columnId)) {
                    columns.get(columnId).add(localDomain);
                } else {
                    columns.add(new MutableIdentifiableIDSet(columnId, localDomain));
                }
            }
        }
        
        // Identify nodes that are outliers in clean columns.
        EQHelper helper = new EQHelper(eqIndex);
        IdentifiableCounterSet counts = new IdentifiableCounterSet();
        for (ExpandedColumn column : columnIndex.columns()) {
            if (strongDomNodes.contains(column.id())) {
                IDSet outliers = column.nodes().difference(strongDomNodes.get(column.id()));
                if (outliers.length() < n) {
                	if ((_strict) && (locDomNodes.contains(column.id()))) {
                		outliers = outliers.difference(locDomNodes.get(column.id()));
                	}
                	int termCount = helper.setSize(outliers);
                	if (termCount < n) {
	                    for (int nodeId : outliers) {
	                        counts.inc(nodeId);
	                    }
                	}
                }
            }
        }
        
        // Get statistics for signatures of NULL value candidates
        SignatureStatsConsumer sigStats = new SignatureStatsConsumer();
        signatures.stream(sigStats, counts.keys());
        
        // Collect statistics for NULL value candidates.
        List<int[]> domStats = new ArrayList<>();
        for (int nodeId : counts.keys()) {
            int candCount = counts.get(nodeId).value();
            int[] stats = new int[]{candCount, 0, 0, 0, nodeId};
            for (int columnId : eqIndex.get(nodeId).columns()) {
                int index = 1;
                if (locDomNodes.contains(columnId)) {
                    if (locDomNodes.get(columnId).contains(nodeId)) {
                        index = 2;
                    }
                }
                if (index == 1) {
                    if (strongDomNodes.contains(columnId)) {
                        if (strongDomNodes.get(columnId).contains(nodeId)) {
                            index = 3;
                        }
                    }
                }
                stats[index] += 1;
            }
            domStats.add(stats);
        }
        Collections.sort(domStats, new StatsSort(sigStats));
        
        // Read terms for NULL value candidates.
        EntitySet terms = termIndexReader.readEntities(eqIndex, counts.keys());
        
        // Write NULL value candidate statistics.
        for (int[] stats : domStats) {
        	EQ node = eqIndex.get(stats[4]);
        	SignatureStats sig = sigStats.get(node.id());
        	for (int termId : node.terms()) {
	        	out.println(
	        			terms.get(termId).name() + "\t" +
						stats[0] + "\t" +
						stats[1] + "\t" +
						stats[2] + "\t" +
						stats[3] + "\t" +
						new FormatedBigDecimal(sig.maxSim()) + "\t" +
						sig.blockCount() + "\t" +
						sig.nodeCount()
				);
        	}
        }
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-file>\n" +
			"  <term-file>\n" +
			"  <signature-file>\n" +
			"  <column-file>\n" +
			"  <local-domain-file>\n" +
			"  <strong-domain-file>\n" +
			"  <strict>\n" +
			"  <output-file>";
	
	private final static Logger LOGGER = Logger
			.getLogger(NullValueCandidateWriter.class.getName());
	
	public static void main(String[] args) {
		
        System.out.println(Constants.NAME + " Null Candidates Writer - Version (" + Constants.VERSION + ")\n");

        if (args.length != 8) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFile = new File(args[0]);
		File termFile = new File(args[1]);
		File signatureFile = new File(args[2]);
		File columnFile = new File(args[3]);
		File localDomainFile = new File(args[4]);
		File strongDomainFile = new File(args[5]);
		boolean strict = Boolean.parseBoolean(args[6]);
		File outputFile = new File(args[7]);
		
		try {
			new NullValueCandidateWriter(strict).run(
					eqFile,
					termFile,
					signatureFile,
					columnFile,
					localDomainFile,
					strongDomainFile,
					outputFile
			);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
