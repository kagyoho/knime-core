
package org.knime.core.data.store.table.row;

import org.knime.core.data.store.column.value.WritableValueAccess;

public interface WritableRow extends AutoCloseable {

	void fwd();

	long getNumValueAccesses();

	WritableValueAccess getValueAccessAt(int index);
}