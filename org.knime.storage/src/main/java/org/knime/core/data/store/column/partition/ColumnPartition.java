package org.knime.core.data.store.column.partition;

public interface ColumnPartition<T> extends AutoCloseable {

	T get();

	long getIndex();

	int getCapacity();

	int getNumValues();

	// TODO I'd really like to not need that
	void setNumValues(int numValues);
}