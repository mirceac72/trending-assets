package com.example.transform.trend;

import com.example.item.CountAndChange;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import java.math.BigDecimal;

import static org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior.FIRE_ALWAYS;

/**
 * <p>
 * ChangeTransform transformation is used to calculate the change between two consecutive values in the
 * sequence of values associated with a key. ChangeTransform creates a new PCollection of KV<String, BigDecimal>
 * where the keys are the keys from the input PCollection and the values are the changes between two consecutive
 * in the sequence of values associated with the key in the input PCollection.
 * </p>
 * <p>
 * ChangeTransform assumes that values in the input PCollection are coming with a specific periodicity and
 * there are no late values.
 * </p>
 */
public class ChangeTransform extends PTransform<PCollection<KV<String, BigDecimal>>, PCollection<KV<String, BigDecimal>>> {

    private Duration inputPeriod;

    /**
     * Creates a new ChangeTransform transformation with the given input period.
     *
     * @param inputPeriod the frequency at which values are expected to arrive in the input PCollection
     */
    public ChangeTransform(Duration inputPeriod) {
        this.inputPeriod = inputPeriod;
    }

    /**
     * Creates a new ChangeTransform transformation with the default input period of 1 hour.
     */
    public ChangeTransform() {
        this(Duration.standardHours(1));
    }

    /**
     * Calculates the change between two consecutive values in the sequence of values associated with a key.
     * If the input sequence of values does not have a value in a specific period then no change will be emitted.
     * If the input sequence of values has more than two values in a specific period then no change will be emitted.
     * <p>
     *   Let us suppose that for a specific key the input PCollection has the following values
     *   (at the timestamps):
     *   <ul>
     *     <li>0 * inputPeriod: v1</li>
     *     <li>1 * inputPeriod: v2</li>
     *     <li>2 * inputPeriod: v3</li>
     *     <li>3 * inputPeriod: v4</li>
     *   </ul>
     *   Then the output PCollection will have the following values:
     *   <ul>
     *     <li>1 * inputPeriod: v2 - v1</li>
     *     <li>2 * inputPeriod: v3 - v2</li>
     *     <li>3 * inputPeriod: v4 - v3</li>
     *    </ul>
     * </p>
     * @param input
     * @return a PCollection that for every key in the input PCollection emits the change between two consecutive
     * values in the sequence of values associated with the key in the input PCollection.
     */
    @Override
    public PCollection<KV<String, BigDecimal>> expand(PCollection<KV<String, BigDecimal>> input) {
        return input.apply("group-consecutive-values", Window.<KV<String, BigDecimal>>into(
                    SlidingWindows.of(inputPeriod.multipliedBy(2))
                        .every(inputPeriod)
                )
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO, FIRE_ALWAYS)
                .discardingFiredPanes()
        )
            .apply("group-consecutive-values", GroupByKey.create())
            .apply("compute-count-and-variation",
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                        TypeDescriptor.of(CountAndChange.class)))
                    .via(keyAndValues -> {
                        var asset = keyAndValues.getKey();
                        int count = 0;
                        var iterator = keyAndValues.getValue().iterator();
                        if (!iterator.hasNext()) {
                            return KV.of(asset, CountAndChange.of(count, BigDecimal.ZERO));
                        }
                        count++;
                        BigDecimal first = iterator.next();
                        if (!iterator.hasNext()) {
                            return KV.of(asset, CountAndChange.of(count, BigDecimal.ZERO));
                        }
                        count++;
                        BigDecimal last = iterator.next();
                        var change = last.subtract(first);
                        if (iterator.hasNext()) {
                            count++;
                        }
                        return KV.of(asset, CountAndChange.of(count, change));
                    }))
            .apply("filter-out-wrong-counts", Filter.by(x -> x.getValue().count == 2))
            .apply("select-change", MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.bigdecimals()))
                .via((KV<String, CountAndChange> x) -> KV.of(x.getKey(), x.getValue().change)));
    }
}
