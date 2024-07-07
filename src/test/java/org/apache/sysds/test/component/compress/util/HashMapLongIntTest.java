/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.component.compress.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.sysds.runtime.compress.utils.HashMapLongInt;
import org.apache.sysds.runtime.compress.utils.HashMapLongInt.KV;
import org.junit.Test;

public class HashMapLongIntTest {

	@Test
	public void add1() {
		addSize(new HashMapLongInt(1));
	}

	@Test
	public void add2() {
		addSize(new HashMapLongInt(2));
	}

	@Test
	public void add4() {
		addSize(new HashMapLongInt(4));
	}

	@Test
	public void add10() {
		addSize(new HashMapLongInt(10));
	}

	@Test
	public void add100() {
		addSize(new HashMapLongInt(100));
	}

	public void addSize(HashMapLongInt a) {
		int r = a.putIfAbsent(1, 1);
		assertEquals(-1, r);
		int r2 = a.putIfAbsent(1, 1);
		assertEquals(1, r2);
		for(int i = 2; i < 10; i++) {

			a.putIfAbsent(i, i);
		}
		assertEquals(9, a.size());
		assertEquals(9, a.putIfAbsent(9, 9));
		Set<Long> s = new HashSet<>();
		Set<Integer> v = new HashSet<>();
		for(KV k : a) {
			s.add(k.k);
			v.add(k.v);
		}
		for(int i = 1; i < 10; i++) {
			assertTrue(s.contains(Long.valueOf(i)));
			assertTrue(v.contains(Integer.valueOf(i)));
		}
		assertEquals(9, s.size());
		assertEquals(4, a.get(4));
		assertEquals(-1, a.get(13));
	}
}
