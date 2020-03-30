
package org.knime.core.data.store.table.column;

import org.knime.core.data.store.Store;

public final class DefaultWritableTable implements WritableTable {

	private final Store m_store;

	public DefaultWritableTable(final Store store) {
		m_store = store;
	}

	@Override
	public long getNumColumns() {
		return m_store.getNumLogicalColumns();
	}

	@Override
	public WritableColumn getColumnAt(final long index) {
		return m_store.getWritableLogicalColumnAt(index);
	}

	@Override
	public void close() throws Exception {
		// TODO
	}
}
