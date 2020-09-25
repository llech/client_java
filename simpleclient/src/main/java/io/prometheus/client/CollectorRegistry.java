package io.prometheus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A registry of Collectors.
 * <p>
 * The majority of users should use the {@link #defaultRegistry}, rather than instantiating their own.
 * <p>
 * Creating a registry other than the default is primarily useful for unittests, or
 * pushing a subset of metrics to the <a href="https://github.com/prometheus/pushgateway">Pushgateway</a>
 * from batch jobs.
 */
public class CollectorRegistry {
  /**
   * The default registry.
   */
  public static final CollectorRegistry defaultRegistry = new CollectorRegistry(true);

  private final Object namesCollectorsLock = new Object();
  private final Map<Collector, List<String>> collectorsToNames = new HashMap<Collector, List<String>>();
  private final Map<String, Collector> namesToCollectors = new HashMap<String, Collector>();

  private final boolean autoDescribe;
  
  /**
   * If set to true, if collector is already registered, and is of the same type, it will be returned (unless overwrite flag is given).
   */
  private final boolean autoReuse;

  public CollectorRegistry() {
    this(false);
  }

  public CollectorRegistry(boolean autoDescribe) {
    this(autoDescribe, false);
  }

  public CollectorRegistry(boolean autoDescribe, boolean autoReuse)
  {
    this.autoDescribe = autoDescribe;
    this.autoReuse = autoReuse;
  }
  
  public <C extends Collector> C register(C m) {
    return register(m, false);
  }

  /**
   * Register a Collector.
   * <p>
   * A collector can be registered to multiple CollectorRegistries.
   */
  @SuppressWarnings("unchecked")
  public <C extends Collector> C register(C m, boolean reuse) {
    List<String> names = collectorNames(m);
    synchronized (collectorsToNames) {
      // first check if there's no conflict on collector name
      for (String name : names) {
        Collector existing = namesToCollectors.get(name);
        if (existing != null) {
          if (!reuse && !autoReuse) {
            throw new IllegalArgumentException("Collector already registered that provides name: " + name);
          } else if (!existing.getClass().equals(m.getClass())) {
            throw new IllegalArgumentException("Unable to reuse collector that provides name: " + name + ". Incompatible types.");
          } else {
            // compare collector names for found collector, overwrite/replace possible only if both sets are identical
            List<String> existingNames = collectorNames(existing);
            if (!existingNames.containsAll(names) || !names.containsAll(existingNames)) {
              throw new IllegalArgumentException("Unable to reuse collector that provides name: " + name + ". Existing collector using that name uses incompatible name set.");
            }
          }
          // if we are here, it means, we have existing collector that should be returned instead of that that we try to register
          return (C) existing;
        }
      }
      for (String name : names) {
        namesToCollectors.put(name, m);
      }
      collectorsToNames.put(m, names);
    }
    return m;
  }

  /**
   * Unregister a Collector.
   */
  public void unregister(Collector m) {
    synchronized (namesCollectorsLock) {
      List<String> names = collectorsToNames.remove(m);
      for (String name : names) {
        namesToCollectors.remove(name);
      }
    }
  }

  /**
   * Unregister all Collectors.
   */
  public void clear() {
    synchronized (namesCollectorsLock) {
      collectorsToNames.clear();
      namesToCollectors.clear();
    }
  }

  /**
   * A snapshot of the current collectors.
   */
  private Set<Collector> collectors() {
    synchronized (namesCollectorsLock) {
      return new HashSet<Collector>(collectorsToNames.keySet());
    }
  }

  private List<String> collectorNames(Collector m) {
    List<Collector.MetricFamilySamples> mfs;
    if (m instanceof Collector.Describable) {
      mfs = ((Collector.Describable) m).describe();
    } else if (autoDescribe) {
      mfs = m.collect();
    } else {
      mfs = Collections.emptyList();
    }

    List<String> names = new ArrayList<String>();
    for (Collector.MetricFamilySamples family : mfs) {
      switch (family.type) {
        case SUMMARY:
          names.add(family.name + "_count");
          names.add(family.name + "_sum");
          names.add(family.name);
          break;
        case HISTOGRAM:
          names.add(family.name + "_count");
          names.add(family.name + "_sum");
          names.add(family.name + "_bucket");
          names.add(family.name);
          break;
        default:
          names.add(family.name);
      }
    }
    return names;
  }

  /**
   * Enumeration of metrics of all registered collectors.
   */
  public Enumeration<Collector.MetricFamilySamples> metricFamilySamples() {
    return new MetricFamilySamplesEnumeration();
  }

  /**
   * Enumeration of metrics matching the specified names.
   * <p>
   * Note that the provided set of names will be matched against the time series
   * name and not the metric name. For instance, to retrieve all samples from a
   * histogram, you must include the '_count', '_sum' and '_bucket' names.
   */
  public Enumeration<Collector.MetricFamilySamples> filteredMetricFamilySamples(Set<String> includedNames) {
    return new MetricFamilySamplesEnumeration(includedNames);
  }

  class MetricFamilySamplesEnumeration implements Enumeration<Collector.MetricFamilySamples> {

    private final Iterator<Collector> collectorIter;
    private Iterator<Collector.MetricFamilySamples> metricFamilySamples;
    private Collector.MetricFamilySamples next;
    private Set<String> includedNames;

    MetricFamilySamplesEnumeration(Set<String> includedNames) {
      this.includedNames = includedNames;
      collectorIter = includedCollectorIterator(includedNames);
      findNextElement();
    }

    private Iterator<Collector> includedCollectorIterator(Set<String> includedNames) {
      if (includedNames.isEmpty()) {
        return collectors().iterator();
      } else {
        HashSet<Collector> collectors = new HashSet<Collector>();
        synchronized (namesCollectorsLock) {
          for (Map.Entry<String, Collector> entry : namesToCollectors.entrySet()) {
            if (includedNames.contains(entry.getKey())) {
              collectors.add(entry.getValue());
            }
          }
        }

        return collectors.iterator();
      }
    }

    MetricFamilySamplesEnumeration() {
      this(Collections.<String>emptySet());
    }

    private void findNextElement() {
      next = null;

      while (metricFamilySamples != null && metricFamilySamples.hasNext()) {
        next = filter(metricFamilySamples.next());
        if (next != null) {
          return;
        }
      }

      if (next == null) {
        while (collectorIter.hasNext()) {
          metricFamilySamples = collectorIter.next().collect().iterator();
          while (metricFamilySamples.hasNext()) {
            next = filter(metricFamilySamples.next());
            if (next != null) {
              return;
            }
          }
        }
      }
    }

    private Collector.MetricFamilySamples filter(Collector.MetricFamilySamples next) {
      if (includedNames.isEmpty()) {
        return next;
      } else {
        Iterator<Collector.MetricFamilySamples.Sample> it = next.samples.iterator();
        while (it.hasNext()) {
            if (!includedNames.contains(it.next().name)) {
                it.remove();
            }
        }
        if (next.samples.size() == 0) {
          return null;
        }
        return next;
      }
    }

    public Collector.MetricFamilySamples nextElement() {
      Collector.MetricFamilySamples current = next;
      if (current == null) {
        throw new NoSuchElementException();
      }
      findNextElement();
      return current;
    }

    public boolean hasMoreElements() {
      return next != null;
    }
  }

  /**
   * Returns the given value, or null if it doesn't exist.
   * <p>
   * This is inefficient, and intended only for use in unittests.
   */
  public Double getSampleValue(String name) {
    return getSampleValue(name, new String[]{}, new String[]{});
  }

  /**
   * Returns the given value, or null if it doesn't exist.
   * <p>
   * This is inefficient, and intended only for use in unittests.
   */
  public Double getSampleValue(String name, String[] labelNames, String[] labelValues) {
    for (Collector.MetricFamilySamples metricFamilySamples : Collections.list(metricFamilySamples())) {
      for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
        if (sample.name.equals(name)
                && Arrays.equals(sample.labelNames.toArray(), labelNames)
                && Arrays.equals(sample.labelValues.toArray(), labelValues)) {
          return sample.value;
        }
      }
    }
    return null;
  }

}
