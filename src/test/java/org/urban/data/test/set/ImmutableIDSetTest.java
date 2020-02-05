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
package org.urban.data.test.set;

import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IDSetMerger;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.set.MultiSetIterator;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ImmutableIDSetTest {
    
    public ImmutableIDSetTest() {
    }
    
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
    public void testContainment() {
    
        IDSet set = new ImmutableIDSet(new Integer[]{1,2,3,5,7,10});
        IDSet emptySet = new ImmutableIDSet(new Integer[]{});
        
        for (int id : new Integer[]{1,2,3,5,7,10}) {
            assertTrue(set.contains(id));
            assertFalse(emptySet.contains(id));
        }
        for (int id : new Integer[]{4,6,8,9}) {
            assertFalse(set.contains(id));
            assertFalse(emptySet.contains(id));
        }
        
        IDSet set1 = new ImmutableIDSet(new Integer[]{13439});
        IDSet set2 = new ImmutableIDSet(new Integer[]{13439, 14179});
        
        assertTrue(set2.contains(set1));
    }
    
    @Test
    public void testMerge() {
        
        ArrayList<ImmutableIDSet> elements = new ArrayList<>();
        elements.add(new ImmutableIDSet(new Integer[]{1,2,3,5,7,10}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,5,6,7}));
        
        assertEquals(8, new IDSetMerger().length(elements));
        ImmutableIDSet merge = new IDSetMerger().merge(elements);
        assertEquals(8, merge.length());
        
        for (int val : new int[]{1,2,3,4,5,6,7,10}) {
            assertTrue(merge.contains(val));
        }
        
        elements = new ArrayList<>();
        elements.add(new ImmutableIDSet(new Integer[]{1,2,3,5,7,10,11}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,5,6,7,11}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,5,6,8,11}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,5,6,7,11}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,9,11}));

        assertEquals(11, new IDSetMerger().length(elements));
        merge = new IDSetMerger().merge(elements);
        assertEquals(11, merge.length());
        
        for (int val = 1; val <= 11; val++) {
            assertTrue(merge.contains(val));
        }
        
        elements = new ArrayList<>();
        elements.add(new ImmutableIDSet(new Integer[]{1,2,3,5,7,10,11}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,5,6,7,11}));
        elements.add(new ImmutableIDSet(new Integer[]{3,4,5,6,8,11}));
        elements.add(new ImmutableIDSet(new Integer[]{2,4,5,6,7,8,12}));
        elements.add(new ImmutableIDSet(new Integer[]{1,9,10,11}));

        MultiSetIterator iter = new MultiSetIterator(elements);
        for (int val = 1; val <=12; val++) {
            assertEquals(val, (int)iter.next());
        }
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testTrim() {
	
        ImmutableIDSet set1 = new ImmutableIDSet(new Integer[]{1,2,3,5,7,10});
        ImmutableIDSet set2 = new ImmutableIDSet(new Integer[]{3,4,5,6,7});
	 
	assertEquals(3, set1.sortedOverlap(set2));
	ImmutableIDSet trim = set1.trim(set2);
	assertEquals(3, trim.length());
	for (int val : new int[]{3,5,7}) {
	    assertTrue(trim.contains(val));
	}
	
	assertEquals(1, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{1,2,3})));
	assertEquals(1, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{7,8,9})));
	assertEquals(1, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{3})));
	assertEquals(1, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{5})));
	assertEquals(1, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{7})));

    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{1,2})));
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{2,4,6})));
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{4,6})));
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{4,6,8})));
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{1})));	
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{2})));	
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{4})));	
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{6})));	
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{8})));	
    	assertEquals(0, trim.sortedOverlap(new ImmutableIDSet(new Integer[]{9,10})));
    }
}
