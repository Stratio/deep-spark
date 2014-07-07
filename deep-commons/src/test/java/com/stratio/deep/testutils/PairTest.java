/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.testutils;

import com.stratio.deep.utils.Pair;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Created by luca on 07/07/14.
 */
@Test
public class PairTest {

	public void testCreation(){
		Pair p = Pair.create("hello", 4D);

		assertNotNull(p);
		assertEquals(p.left, "hello");
		assertEquals(p.right, new Double(4));
	}

	public void testEquals(){
		Pair p1 = Pair.create("hello", 1D);
		Pair p2 = Pair.create("hello2D", 2D);
		Pair p3 = Pair.create("hello", 3D);
		Pair p4 = Pair.create("hello", 1D);

		assertFalse(p1.equals(new Integer(1)));
		assertFalse(p1.equals(p2));
		assertFalse(p1.equals(p3));
		assertTrue(p1.equals(p4));
	}
}
