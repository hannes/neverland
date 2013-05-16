package nl.cwi.da.neverland.internal;

import java.text.DecimalFormat;
import java.util.List;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class StatisticalDescription {
	public double min = 0;
	public double max = 0;
	public double mean = 0;
	public double median = 0;
	public long count = 0;
	public double sum = 0;

	private DecimalFormat df = new DecimalFormat("#.##");

	private DescriptiveStatistics stats = new DescriptiveStatistics();

	public StatisticalDescription(List<Double> values) {
		if (values == null || values.size() == 0) {
			return;
		}
		for (Double v : values) {
			addValue(v);
		}
		calculate();
	}

	public StatisticalDescription() {
	}

	public void addValue(double value) {
		stats.addValue(value);
	}

	public void merge(StatisticalDescription osd) {
		double[] values = osd.getValues();
		for (int i = 0; i < values.length; i++) {
			stats.addValue(values[i]);
		}
		calculate();
	}

	public double[] getValues() {
		return stats.getValues();
	}

	public StatisticalDescription calculate() {
		min = stats.getMin();
		max = stats.getMax();
		mean = stats.getMean();
		median = stats.getPercentile(50);
		count = stats.getN();
		sum = stats.getSum();
		return this;
	}

	@Override
	public String toString() {
		return "min=" + df.format(min) + "\tmax=" + df.format(max) + "\tmean="
				+ df.format(mean) + "\tmedian=" + df.format(median)
				+ "\tcount=" + df.format(count);
	}
}