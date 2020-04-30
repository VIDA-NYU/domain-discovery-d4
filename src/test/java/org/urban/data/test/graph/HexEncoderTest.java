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
package org.urban.data.test.graph;

import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ImmutableExpandedColumn;
import org.opendata.curation.d4.domain.graph.HexEdgeReader;
import org.opendata.curation.d4.domain.graph.HexEdgeWriter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HexEncoderTest {
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testHexEncode() {
        
        HashIDSet nodes = new HashIDSet(new int[]{36766, 36767});
        HashIDSet expansion = new HashIDSet();
        
        ExpandedColumn column = new ImmutableExpandedColumn(0, nodes, expansion);
        HexEdgeWriter writer = new HexEdgeWriter(column);
        HexEdgeReader reader = new HexEdgeReader(column);
        
        String hexString = writer.toHexString("36767");
        assertEquals(hexString, "4");
        int[] edges = reader.parseLine(hexString);
        assertEquals(edges.length, 1);
        assertEquals(edges[0], 36767);
    }
}
