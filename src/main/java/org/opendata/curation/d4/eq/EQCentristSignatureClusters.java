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
package org.opendata.curation.d4.eq;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.ImmutableIdentifiableIDSet;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.trim.CentristTrimmer;
import org.opendata.db.Database;
import org.opendata.db.column.ColumnNameReader;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

public class EQCentristSignatureClusters {

	private final Database _db;
	private final EQIndex _eqIndex;
	
	public EQCentristSignatureClusters(EQIndex eqIndex) {
	
		_eqIndex = eqIndex;
		_db = new Database(_eqIndex);
	}
	
	public IdentifiableObjectSet<IdentifiableIDSet> run(int nodeId) {	
		
		EQ node = _eqIndex.get(nodeId);
		
		SignatureBlocksIndex buffer;
		buffer = new SignatureBlocksIndex();

		ConcurrentLinkedQueue<Integer> queue;
		queue = new ConcurrentLinkedQueue<>();
		queue.add(nodeId);
		try {
			new SignatureBlocksGenerator()
				.runWithMaxDrop(_eqIndex, queue, true, true, 1, buffer);
		} catch (java.lang.InterruptedException | java.io.IOException ex) {
			throw new RuntimeException(ex);
		}
		
		SignatureBlocks signature = buffer.get(nodeId);
		
		int[] nodeSizes = _eqIndex.nodeSizes();
		
		HashMap<String, HashIDSet> clusters = new HashMap<>();
		for (int columnId : node.columns()) {
			buffer = new SignatureBlocksIndex();
			CentristTrimmer trimmer;
			trimmer = new CentristTrimmer(_db.columns().get(columnId), nodeSizes, buffer);
			trimmer.open();
			trimmer.consume(signature);
			trimmer.close();
			String key = buffer.get(nodeId).nodes().toIntString();
			if (!clusters.containsKey(key)) {
				clusters.put(key, new HashIDSet());
			}
			clusters.get(key).add(columnId);
		}
		
		HashObjectSet<IdentifiableIDSet> result = new HashObjectSet<>();
		for (HashIDSet cluster : clusters.values()) {
			result.add(new ImmutableIdentifiableIDSet(result.length(), cluster));
		}
		return result;
	}
	
	private final static String COMMAND =
			"Usage:\n" +
			"  <eq-file>\n" +
			"  <column-names-file>\n"  +
			"  <node-id>";
	
	private final static Logger LOGGER = Logger
			.getLogger(EQCentristSignatureClusters.class.getName());
	
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println(COMMAND);
			System.exit(-1);
		}
		
		File eqFile = new File(args[0]);
		File columnsFile = new File(args[1]);
		int nodeId = Integer.parseInt(args[2]);
		
		EQIndex eqIndex = null;
		try {
			eqIndex = new EQIndex(eqFile);
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, eqFile.getName(), ex);
			System.exit(-1);
		}
		
		EntitySet columnNames = null;
		try {
			columnNames = new ColumnNameReader(columnsFile).read();
		} catch (java.io.IOException ex) {
			LOGGER.log(Level.SEVERE, columnsFile.getName(), ex);
			System.exit(-1);
		}
		
		IdentifiableObjectSet<IdentifiableIDSet> clusters;
		clusters = new EQCentristSignatureClusters(eqIndex).run(nodeId);
		System.out.println("GOT " + clusters.length() + " CLUSTERS");
		
		for (IdentifiableIDSet cluster : clusters) {
			System.out.println("\nCLUSTER\n-------");
			for (int columnId : cluster) {
				System.out.println(columnId + "\t" + columnNames.get(columnId).name());
			}
		}
	}
}
