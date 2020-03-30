
package org.knime.core.data.store.knime;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.ArrowStore;
import org.knime.core.data.store.Store;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.table.column.DefaultReadableTable;
import org.knime.core.data.store.table.column.DefaultWritableTable;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.row.ReadableRowIterator;
import org.knime.core.data.store.table.row.ReadableValueAccess;
import org.knime.core.data.store.table.row.Row;
import org.knime.core.data.store.table.row.WritableRowIterator;
import org.knime.core.data.store.table.row.WritableValueAccess;
import org.knime.core.data.store.vec.rw.ReadableDoubleValueAccess;
import org.knime.core.data.store.vec.rw.WritableDoubleValueAccess;

public class StorageTest {

	private static final ColumnSchema doubleVectorSchema = () -> ColumnType.DOUBLE;

//	@Test
//	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
//		final long numRows = 100_000_000;
//
//		try (final WritableTable table = TableFactory.createWritableTable(doubleVectorSchema)) {
//			try (final WritableColumn column = (WritableColumn) table.getColumnAt(0)) {
//				for (long i = 0; i < numRows; i++) {
//					column.fwd();
//					if (i % numRows / 100 == 0) {
//						column.setMissing();
//					} else {
//						column.setDoubleValue(i);
//					}
//				}
//			}
//		}
//
//		// TODO: Somehow transfer underlying table store from writable table above
//		// to readable table here.
//		try (final ReadableTable table = TableFactory.createReadableTable()) {
//			try (final ReadableDoubleColumn column = (ReadableDoubleColumn) table.getColumnAt(0)) {
//				for (long i = 0; column.canFwd(); i++) {
//					column.fwd();
//					if (i % numRows / 100 == 0) {
//						Assert.assertTrue(column.isMissing());
//					} else {
//						Assert.assertEquals(i, column.getDoubleValue(), 0.0000001);
//					}
//				}
//			}
//		}
//	}

	/**
	 * Somewhat close to KNIME's original table API. The underlying implementation
	 * already uses row and data-value proxies, so we could pre-cast them and only
	 * fwd the iterators. TODO: Move casting etc. outside of loop.
	 */
	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 1000;
		final Store store = new ArrowStore(new ColumnSchema[] { doubleVectorSchema });

		final WritableTable writableTable = new DefaultWritableTable(store);
		try (final WritableRowIterator iterator = TableFactory.createRowWriteAccess(writableTable)) {
			for (long i = 0; i < numRows; i++) {
				final Row<WritableValueAccess> row = iterator.next();
				final WritableDoubleValueAccess value = (WritableDoubleValueAccess) row.getValueAt(0);
				if (i % numRows / 100 == 0) {
					value.setMissing();
				}
				else {
					value.setDoubleValue(i);
				}
			}
		}

		final ReadableTable readableTable = new DefaultReadableTable(store);
		try (final ReadableRowIterator iterator = TableFactory.createRowReadAccess(readableTable)) {
			for (long i = 0; iterator.hasNext(); i++) {
				final Row<ReadableValueAccess> row = iterator.next();
				final ReadableDoubleValueAccess value = (ReadableDoubleValueAccess) row.getValueAt(0);
				if (i % numRows / 100 == 0) {
					Assert.assertTrue(value.isMissing());
				}
				else {
					Assert.assertEquals(i, value.getDoubleValue(), 0.0000001);
				}
			}
		}
	}

	/*
	 *
	 * MOCKS TO MIMIC KNIME API
	 *
	 */

//	@Test
//	public void pushViaKNIMEAPI() {
//		final DataContainer container = new DataContainer() {
//
//			{
//				final WritableTable table = TableFactory.createWritableTable(convert(getSpec()));
//			}
//
//			private ColumnSchema convert(final DataTableSpec spec) {
//				return null;
//			}
//
//			@Override
//			public void addRowToTable(final DataRow row) {}
//
//			@Override
//			public DataTableSpec getSpec() {
//				return null;
//			}
//		};
//	}
//
//	interface DataContainer {
//
//		void addRowToTable(DataRow row);
//
//		DataTableSpec getSpec();
//	}
//
//	interface DataTableSpec {
//
//	}
//
//	interface BufferedDataTable extends Iterable<DataRow> {
//
//	}
//
//	interface DataRow {
//
//		String getRowKey();
//
//		DataCell getCell(int i);
//
//		int numCells();
//
//		// TODO more stuff
//	}
//
//	class DataCell implements DataValue {
//
//	}
//
//	interface DataValue {
//
//	}
//
//	interface WritableDataValue {
//
//	}
}
