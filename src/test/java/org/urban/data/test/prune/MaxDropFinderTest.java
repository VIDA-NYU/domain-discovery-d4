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
package org.urban.data.test.prune;

import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.MaxDropFinder;

/**
 * Unit tests for the steepest drop finder.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MaxDropFinderTest {
    
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
    public void testDropFinder() {
	
        MaxDropFinder<IdentifiableDouble> dropFinder;
        dropFinder = new MaxDropFinder<>(0.5, true, true);

        // Ensure that empty lists are handeled correctly
        assertEquals(0, dropFinder.getPruneIndex(new ArrayList<>()));
        
        ArrayList<IdentifiableDouble> elements = new ArrayList<>();
	elements.add(new IdentifiableDouble(10, 0.75));
	elements.add(new IdentifiableDouble(1, 0.65));
	elements.add(new IdentifiableDouble(2, 0.6));
	elements.add(new IdentifiableDouble(3, 0.49));
	elements.add(new IdentifiableDouble(4, 0.48));
	elements.add(new IdentifiableDouble(5, 0.45));
	elements.add(new IdentifiableDouble(6, 0.4));

        // Ensure that the full signature constraint is triggered
	assertEquals(elements.size(), dropFinder.getPruneIndex(elements));
	assertEquals(3, new MaxDropFinder<>(0.5, false, true).getPruneIndex(elements));
        
        // Ensure that the ignore last drop constraint works
	assertEquals(elements.size(), new MaxDropFinder<>(0.5, false, false).getPruneIndex(elements));
        
        // Ensure that the empty signature constraint works
	assertEquals(0, new MaxDropFinder<>(0.8, true, true).getPruneIndex(elements));
        
        elements.add(new IdentifiableDouble(7, 0.35));
        elements.add(new IdentifiableDouble(8, 0.3));
        elements.add(new IdentifiableDouble(9, 0.25));

        // The largest drop is between elements 2 and 3
	assertEquals(3, dropFinder.getPruneIndex(elements));
        
        elements.add(3, new IdentifiableDouble(10, 0.55));
        
         // The largest drop is now between elements 0 and 1
	assertEquals(1, dropFinder.getPruneIndex(elements));
   }

    @Test
    public void testNextToLastDrop() {
	
        MaxDropFinder<IdentifiableDouble> dropFinder;
        dropFinder = new MaxDropFinder<>(0.5, false, true);
        
        ArrayList<IdentifiableDouble> elements = new ArrayList<>();
	elements.add(new IdentifiableDouble(0, 0.75));
	elements.add(new IdentifiableDouble(0, 0.65));
	elements.add(new IdentifiableDouble(0, 0.6));
	elements.add(new IdentifiableDouble(0, 0.4));
        
        // The drop occurs before the last element
 	assertEquals(elements.size() - 1, dropFinder.getPruneIndex(elements));
       
        // The ignore last drop constraint changes the result
 	assertEquals(elements.size(), new MaxDropFinder<>(0.5, false, false).getPruneIndex(elements));
   }

    @Test
    public void testSingleElementDropFinder() {
        
        ArrayList<IdentifiableDouble> elements = new ArrayList<>();
	elements.add(new IdentifiableDouble(0, 0.75));
        
        // Ensure that the empty signature constraint works
	assertEquals(0, new MaxDropFinder<>(0.8, true, true).getPruneIndex(elements));
        
        // The result is one if the empty signature constraint is not triggered
	assertEquals(1, new MaxDropFinder<>(0.6, true, true).getPruneIndex(elements));
	assertEquals(1, new MaxDropFinder<>(0.6, true, false).getPruneIndex(elements));
	assertEquals(1, new MaxDropFinder<>(0.6, false, true).getPruneIndex(elements));
	assertEquals(1, new MaxDropFinder<>(0.6, false, false).getPruneIndex(elements));
    }

    @Test
    public void testVaryStartDropFinder() {
	
        MaxDropFinder<IdentifiableDouble> dropFinder;
        dropFinder = new MaxDropFinder<>(0.5, false, true);

        ArrayList<IdentifiableDouble> elements = new ArrayList<>();
	elements.add(new IdentifiableDouble(0, 0.75));
	elements.add(new IdentifiableDouble(1, 0.65));
	elements.add(new IdentifiableDouble(2, 0.6));
	elements.add(new IdentifiableDouble(1, 0.49));
	elements.add(new IdentifiableDouble(1, 0.48));
	elements.add(new IdentifiableDouble(1, 0.45));
	elements.add(new IdentifiableDouble(1, 0.4));

        // Ensure that the full signature constraint is triggered
	assertEquals(3, dropFinder.getPruneIndex(elements, 0));
        assertEquals(6, dropFinder.getPruneIndex(elements, 3));
        assertEquals(7, dropFinder.getPruneIndex(elements, 6));
        assertEquals(0, dropFinder.getPruneIndex(elements, 7));
   }

}
