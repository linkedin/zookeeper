package org.apache.zookeeper.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class SegmentedList<E> {

  private final int segmentCount;
  private final List<E>[] segments;
  private final ReentrantLock[] locks;

  @SuppressWarnings("unchecked")
  public SegmentedList(int segmentCount) {
    this.segmentCount = segmentCount;
    this.segments = (List<E>[]) new List[segmentCount];
    this.locks = new ReentrantLock[segmentCount];

    for (int i = 0; i < segmentCount; i++) {
      segments[i] = new ArrayList<>();
      locks[i] = new ReentrantLock();
    }
  }

  private int segmentIndex(Object element) {
    return (element == null ? 0 : (element.hashCode() & 0x7fffffff) % segmentCount);
  }

  public void add(E element) {
    int seg = segmentIndex(element);
    locks[seg].lock();
    try {
      segments[seg].add(element);
    } finally {
      locks[seg].unlock();
    }
  }

  public List<E> toListSnapshot() {
    // snapshot copied under all locks to ensure consistency
    List<E> snapshot = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      locks[i].lock();
    }
    try {
      for (List<E> segment : segments) {
        snapshot.addAll(segment);
      }
    } finally {
      for (int i = segmentCount - 1; i >= 0; i--) {
        locks[i].unlock();
      }
    }
    return snapshot;
  }

  public int size() {
    int size = 0;
    for (int i = 0; i < segmentCount; i++) {
      locks[i].lock();
    }
    try {
      for (List<E> segment : segments) {
        size += segment.size();
      }
    } finally {
      for (int i = segmentCount - 1; i >= 0; i--) {
        locks[i].unlock();
      }
    }
    return size;
  }
}
