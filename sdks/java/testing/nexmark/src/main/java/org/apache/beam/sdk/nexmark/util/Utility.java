/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.nexmark.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Utility {

	private Utility() {
		// This should not be called.
	}

	public static <T> T kthSmallestElement(final List<T> list,
										   final int k,
										   final Comparator<T> comparator) {
		int pivotIndex = selectPivot(list);
		final T pivot = list.get(pivotIndex);
		for (int i = 1; i < list.size(); i++) {
			if (comparator.compare(pivot, list.get(i)) > 0) {
				Collections.swap(list, pivotIndex, i);
				pivotIndex = i;
			}
		}
		if (k < pivotIndex + 1) {
			return kthSmallestElement(list.subList(0, pivotIndex), k, comparator);
		} else if (pivotIndex + 1 == k) {
			return pivot;
		} else {
			return kthSmallestElement(list.subList(pivotIndex+1, list.size()), k - (pivotIndex + 1), comparator);
		}
	}

	private static <T> int selectPivot(final List<T> list) {
		return 0;
	}
}
