//! Go `sort.Sort` (pattern-defeating quicksort), ported bit-for-bit so that callers whose
//! comparators have ties (e.g. `go-sockaddr`'s `OrderedIfAddrBy`) produce exactly the same
//! (unstable) order as upstream Go.
//!
//! Ported from Go's standard library: `src/sort/sort.go` (`Sort`, `xorshift`,
//! `nextPowerOfTwo`) and `src/sort/zsortinterface.go` (`insertionSort`, `siftDown`,
//! `heapSort`, `pdqsort`, `partition`, `partitionEqual`, `partialInsertionSort`,
//! `breakPatterns`, `choosePivot`, `order2`, `median`, `medianAdjacent`, `reverseRange`).

/// Mirror of Go's `sort.Interface`.
#[allow(clippy::len_without_is_empty)]
pub trait Interface {
    fn len(&self) -> usize;
    fn less(&self, i: usize, j: usize) -> bool;
    fn swap(&mut self, i: usize, j: usize);
}

/// Adapter sorting a slice with a Go-style `Less` closure.
struct SliceLess<'a, T, F: Fn(&T, &T) -> bool> {
    data: &'a mut [T],
    less: F,
}

impl<T, F: Fn(&T, &T) -> bool> Interface for SliceLess<'_, T, F> {
    fn len(&self) -> usize {
        self.data.len()
    }
    fn less(&self, i: usize, j: usize) -> bool {
        (self.less)(&self.data[i], &self.data[j])
    }
    fn swap(&mut self, i: usize, j: usize) {
        self.data.swap(i, j)
    }
}

/// Go `sort.Slice`-equivalent entry point: sorts `data` with Go's exact (unstable) `sort.Sort`
/// algorithm using `less`.
pub fn sort_by<T>(data: &mut [T], less: impl Fn(&T, &T) -> bool) {
    sort(&mut SliceLess { data, less });
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum SortedHint {
    Unknown,
    Increasing,
    Decreasing,
}

/// `bits.Len(uint(x))`.
fn bits_len(x: usize) -> usize {
    (usize::BITS - x.leading_zeros()) as usize
}

/// Go `sort.Sort`.
pub fn sort<D: Interface + ?Sized>(data: &mut D) {
    let n = data.len();
    if n <= 1 {
        return;
    }
    let limit = bits_len(n);
    pdqsort(data, 0, n, limit);
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn next_power_of_two(length: usize) -> usize {
    let shift = bits_len(length);
    1usize << shift
}

fn insertion_sort<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize) {
    for i in a + 1..b {
        let mut j = i;
        while j > a && data.less(j, j - 1) {
            data.swap(j, j - 1);
            j -= 1;
        }
    }
}

fn sift_down<D: Interface + ?Sized>(data: &mut D, lo: usize, hi: usize, first: usize) {
    let mut root = lo;
    loop {
        let mut child = 2 * root + 1;
        if child >= hi {
            break;
        }
        if child + 1 < hi && data.less(first + child, first + child + 1) {
            child += 1;
        }
        if !data.less(first + root, first + child) {
            return;
        }
        data.swap(first + root, first + child);
        root = child;
    }
}

fn heap_sort<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize) {
    let first = a;
    let lo = 0;
    let hi = b - a;

    let mut i = (hi as isize - 1) / 2;
    while i >= 0 {
        sift_down(data, i as usize, hi, first);
        i -= 1;
    }

    let mut i = hi as isize - 1;
    while i >= 0 {
        data.swap(first, first + i as usize);
        sift_down(data, lo, i as usize, first);
        i -= 1;
    }
}

fn pdqsort<D: Interface + ?Sized>(data: &mut D, mut a: usize, mut b: usize, mut limit: usize) {
    const MAX_INSERTION: usize = 12;

    let mut was_balanced = true;
    let mut was_partitioned = true;

    loop {
        let length = b - a;

        if length <= MAX_INSERTION {
            insertion_sort(data, a, b);
            return;
        }

        if limit == 0 {
            heap_sort(data, a, b);
            return;
        }

        if !was_balanced {
            break_patterns(data, a, b);
            limit -= 1;
        }

        let (mut pivot, mut hint) = choose_pivot(data, a, b);
        if hint == SortedHint::Decreasing {
            reverse_range(data, a, b);
            pivot = (b - 1) - (pivot - a);
            hint = SortedHint::Increasing;
        }

        if was_balanced
            && was_partitioned
            && hint == SortedHint::Increasing
            && partial_insertion_sort(data, a, b)
        {
            return;
        }

        if a > 0 && !data.less(a - 1, pivot) {
            let mid = partition_equal(data, a, b, pivot);
            a = mid;
            continue;
        }

        let (mid, already_partitioned) = partition(data, a, b, pivot);
        was_partitioned = already_partitioned;

        let (left_len, right_len) = (mid - a, b - mid);
        let balance_threshold = length / 8;
        if left_len < right_len {
            was_balanced = left_len >= balance_threshold;
            pdqsort(data, a, mid, limit);
            a = mid + 1;
        } else {
            was_balanced = right_len >= balance_threshold;
            pdqsort(data, mid + 1, b, limit);
            b = mid;
        }
    }
}

fn partition<D: Interface + ?Sized>(
    data: &mut D,
    a: usize,
    b: usize,
    pivot: usize,
) -> (usize, bool) {
    data.swap(a, pivot);
    // Signed indices: Go's `j` may drop below `i` (and `a`) transiently.
    let (mut i, mut j) = (a as isize + 1, b as isize - 1);

    while i <= j && data.less(i as usize, a) {
        i += 1;
    }
    while i <= j && !data.less(j as usize, a) {
        j -= 1;
    }
    if i > j {
        data.swap(j as usize, a);
        return (j as usize, true);
    }
    data.swap(i as usize, j as usize);
    i += 1;
    j -= 1;

    loop {
        while i <= j && data.less(i as usize, a) {
            i += 1;
        }
        while i <= j && !data.less(j as usize, a) {
            j -= 1;
        }
        if i > j {
            break;
        }
        data.swap(i as usize, j as usize);
        i += 1;
        j -= 1;
    }
    data.swap(j as usize, a);
    (j as usize, false)
}

fn partition_equal<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize, pivot: usize) -> usize {
    data.swap(a, pivot);
    let (mut i, mut j) = (a as isize + 1, b as isize - 1);

    loop {
        while i <= j && !data.less(a, i as usize) {
            i += 1;
        }
        while i <= j && data.less(a, j as usize) {
            j -= 1;
        }
        if i > j {
            break;
        }
        data.swap(i as usize, j as usize);
        i += 1;
        j -= 1;
    }
    i as usize
}

fn partial_insertion_sort<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize) -> bool {
    const MAX_STEPS: usize = 5;
    const SHORTEST_SHIFTING: usize = 50;
    let mut i = a + 1;
    for _ in 0..MAX_STEPS {
        while i < b && !data.less(i, i - 1) {
            i += 1;
        }

        if i == b {
            return true;
        }

        if b - a < SHORTEST_SHIFTING {
            return false;
        }

        data.swap(i, i - 1);

        if i - a >= 2 {
            let mut j = i - 1;
            while j >= 1 {
                if !data.less(j, j - 1) {
                    break;
                }
                data.swap(j, j - 1);
                j -= 1;
            }
        }
        if b - i >= 2 {
            let mut j = i + 1;
            while j < b {
                if !data.less(j, j - 1) {
                    break;
                }
                data.swap(j, j - 1);
                j += 1;
            }
        }
    }
    false
}

fn break_patterns<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize) {
    let length = b - a;
    if length >= 8 {
        let mut random = XorShift(length as u64);
        let modulus = next_power_of_two(length);

        let mut idx = a + (length / 4) * 2 - 1;
        while idx <= a + (length / 4) * 2 + 1 {
            let mut other = (random.next() as usize) & (modulus - 1);
            if other >= length {
                other -= length;
            }
            data.swap(idx, a + other);
            idx += 1;
        }
    }
}

fn choose_pivot<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize) -> (usize, SortedHint) {
    const SHORTEST_NINTHER: usize = 50;
    const MAX_SWAPS: usize = 4 * 3;

    let l = b - a;

    let mut swaps = 0usize;
    let mut i = a + l / 4;
    let mut j = a + l / 4 * 2;
    let mut k = a + l / 4 * 3;

    if l >= 8 {
        if l >= SHORTEST_NINTHER {
            i = median_adjacent(data, i, &mut swaps);
            j = median_adjacent(data, j, &mut swaps);
            k = median_adjacent(data, k, &mut swaps);
        }
        j = median(data, i, j, k, &mut swaps);
    }

    match swaps {
        0 => (j, SortedHint::Increasing),
        MAX_SWAPS => (j, SortedHint::Decreasing),
        _ => (j, SortedHint::Unknown),
    }
}

fn order2<D: Interface + ?Sized>(data: &D, a: usize, b: usize, swaps: &mut usize) -> (usize, usize) {
    if data.less(b, a) {
        *swaps += 1;
        return (b, a);
    }
    (a, b)
}

fn median<D: Interface + ?Sized>(data: &D, a: usize, b: usize, c: usize, swaps: &mut usize) -> usize {
    let (a, b) = order2(data, a, b, swaps);
    let (b, c) = order2(data, b, c, swaps);
    let _ = c;
    let (_a, b) = order2(data, a, b, swaps);
    b
}

fn median_adjacent<D: Interface + ?Sized>(data: &D, a: usize, swaps: &mut usize) -> usize {
    median(data, a - 1, a, a + 1, swaps)
}

fn reverse_range<D: Interface + ?Sized>(data: &mut D, a: usize, b: usize) {
    let mut i = a;
    let mut j = b - 1;
    while i < j {
        data.swap(i, j);
        i += 1;
        j -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_like_a_sort() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        for n in [0usize, 1, 2, 5, 12, 13, 49, 50, 51, 200, 1000] {
            let mut v: Vec<u32> = (0..n)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    (seed % 37) as u32
                })
                .collect();
            let mut expected = v.clone();
            expected.sort();
            sort_by(&mut v, |a, b| a < b);
            assert_eq!(v, expected, "n={n}");
        }
        let mut rev: Vec<u32> = (0..500).rev().collect();
        sort_by(&mut rev, |a, b| a < b);
        assert_eq!(rev, (0..500).collect::<Vec<_>>());
    }

    /// Insertion sort (n <= 12) is stable, like Go.
    #[test]
    fn small_inputs_are_stable() {
        let mut v: Vec<(u8, usize)> = vec![(1, 0), (0, 1), (1, 2), (0, 3), (1, 4), (0, 5)];
        sort_by(&mut v, |a, b| a.0 < b.0);
        assert_eq!(v, vec![(0, 1), (0, 3), (0, 5), (1, 0), (1, 2), (1, 4)]);
    }
}
