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
import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.constraint.Threshold;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.db.eq.EQHelper;
import org.opendata.db.eq.EQIndex;

/**
 * Consumer that merges domains with overlap above a given threshold.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public class OverlappingDomainMerger implements DomainConsumer {

	private final DomainConsumer _consumer;
	private final HashObjectSet<Domain> _domains;
	private final EQHelper _nodes;
	private final Threshold _threshold;
	
	public OverlappingDomainMerger(
			EQHelper nodes,
			Threshold threshold,
			DomainConsumer consumer
	) {
		_nodes = nodes;
		_threshold = threshold;
		_consumer = consumer;
		
		_domains = new HashObjectSet<>();
	}
	
	@Override
	public void close() {

		System.out.println("READ " + _domains.length() + " DOMAINS");
		
		UndirectedConnectedComponents compGen;
		compGen = new UndirectedConnectedComponents(_domains.keys());
		
		List<Domain> domainList = _domains.toList();
		for (int iDomain = 0; iDomain < domainList.size() - 1; iDomain++) {
			Domain domI = domainList.get(iDomain);
			for (int jDomain = iDomain + 1; jDomain < domainList.size(); jDomain++) {
				Domain domJ = domainList.get(jDomain);
				BigDecimal ji;
				ji = _nodes.getJI(domI, domJ);
				if (_threshold.isSatisfied(ji)) {
					compGen.edge(domI.id(), domJ.id());
				} else if (ji.compareTo(BigDecimal.ZERO) > 0) {
					System.out.println("Reject merge of " + domI.id() + " with " + domJ.id() + " at " + ji.toPlainString());
				}
			}
		}
		
		_consumer.open();
		for (IdentifiableIDSet comp : compGen.getComponents()) {
			if (comp.length() > 1) {
				HashIDSet nodes = new HashIDSet();
				HashIDSet columns = new HashIDSet();
				for (int domainId : comp) {
					Domain dom = _domains.get(domainId);
					nodes.add(dom);
					columns.add(dom.columns());
				}
				_consumer.consume(new Domain(comp.id(), nodes, columns));
			} else {
				_consumer.consume(_domains.get(comp.id()));
			}
		}
		_consumer.close();
	}

	@Override
	public void consume(Domain domain) {

		_domains.add(domain);
	}

	@Override
	public void open() {

		_domains.clear();
	}
	
	private final static String COMMAND =
			"Usage\n" +
			"  <eq-file>\n" +
			"  <domains-file>\n" +
			"  <threshold>\n" +
			"  <output-file>";
	
	private final static Logger LOGGER = Logger
			.getLogger(OverlappingDomainMerger.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 4) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFile = new File(args[0]);
		File inputFile = new File(args[1]);
		Threshold threshold = Threshold.getConstraint(args[2]);
		File outputFile = new File(args[3]);

		DomainReader reader = new DomainReader(inputFile);
		
		try {
			EQHelper nodes = new EQHelper(new EQIndex(eqFile));
			DomainConsumer consumer = new DomainWriter(outputFile);
			consumer = new DomainSetStatsPrinter(consumer);
			consumer = new OverlappingDomainMerger(nodes, threshold, consumer);
			reader.stream(consumer);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, "RUN", ex);
			System.exit(-1);
		}
	}
}
