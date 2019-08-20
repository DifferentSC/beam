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
