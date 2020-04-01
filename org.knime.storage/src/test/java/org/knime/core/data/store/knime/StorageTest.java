
package org.knime.core.data.store.knime;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.arrow.ArrowTable;
import org.knime.core.data.store.arrow.ArrowUtils;
import org.knime.core.data.store.column.ColumnSchema;
import org.knime.core.data.store.column.ColumnType;
import org.knime.core.data.store.column.ReadableColumnCursor;
import org.knime.core.data.store.column.WritableColumn;
import org.knime.core.data.store.column.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.column.value.WritableDoubleValueAccess;
import org.knime.core.data.store.table.row.ColumnBackedReadableRow;
import org.knime.core.data.store.table.row.ColumnBackedWritableRow;
import org.knime.core.data.store.table.row.ReadableRow;
import org.knime.core.data.store.table.row.WritableRow;

public class StorageTest {

	/*
	 * TODO later we have to obviously parametrize the tests independently. for now
	 * we're good.
	 */

	// in numValues per vector
	private static final int BATCH_SIZE = 5_000_00;

	// in bytes
	private static final long OFFHEAP_SIZE = 2048_000_000;

	// num rows used for testing
	private static final long NUM_ROWS = 1_000_000;

	// some schema
	private static final ColumnSchema[] SCHEMAS = new ColumnSchema[] { () -> ColumnType.DOUBLE,
			() -> ColumnType.DOUBLE };

	@Test
	public void doubleArrayTest() {
		final double[] array = new double[100_000_000];
		for (int i = 0; i < array.length; i++) {
			array[i] = i;
		}

		for (int i = 0; i < array.length; i++) {
			double k = array[i];
			Assert.assertEquals(array[i], k, 0.00000000000001);
		}
	}

	@Test
	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {

		try (final ArrowTable table = ArrowUtils.createArrowTable(BATCH_SIZE, OFFHEAP_SIZE, SCHEMAS)) {

			long time = System.currentTimeMillis();
			// first column write
			try (final WritableColumn col0 = table.getWritableColumn(0);
					final WritableColumn col1 = table.getWritableColumn(1)) {
				final WritableDoubleValueAccess val0 = (WritableDoubleValueAccess) col0.getValueAccess();
				final WritableDoubleValueAccess val1 = (WritableDoubleValueAccess) col1.getValueAccess();
				for (long i = 0; i < NUM_ROWS; i++) {
					col0.fwd();
					col1.fwd();
					if (i % 100 == 0) {
						val0.setMissing();
						val1.setDoubleValue(i);
					} else {
						val0.setDoubleValue(i);
						val1.setMissing();
					}
				}
			}

			// then read
			try (final ReadableColumnCursor col0 = table.getReadableColumn(0).cursor();
					final ReadableColumnCursor col1 = table.getReadableColumn(1).cursor()) {
				final ReadableDoubleValueAccess val0 = (ReadableDoubleValueAccess) col0.getValueAccess();
				final ReadableDoubleValueAccess val1 = (ReadableDoubleValueAccess) col1.getValueAccess();
				for (long i = 0; col0.canFwd(); i++) {
					col0.fwd();
					col1.fwd();
					if (i % 100 == 0) {
						Assert.assertTrue(val0.isMissing());
						Assert.assertEquals(i, val1.getDoubleValue(), 0.0000001);
					} else {
						Assert.assertEquals(i, val0.getDoubleValue(), 0.0000001);
						Assert.assertTrue(val1.isMissing());
					}
				}
			}
			System.out.println((System.currentTimeMillis() - time));

			// And read again row-wise
			try (final ReadableRow row = ColumnBackedReadableRow.fromReadableTable(table)) {
				final ReadableDoubleValueAccess val0 = (ReadableDoubleValueAccess) row.getValueAccessAt(0);
				final ReadableDoubleValueAccess val1 = (ReadableDoubleValueAccess) row.getValueAccessAt(1);
				for (long i = 0; row.canFwd(); i++) {
					row.fwd();
					if (i % 100 == 0) {
						Assert.assertTrue(val0.isMissing());
						Assert.assertEquals(i, val1.getDoubleValue(), 0.0000001);
					} else {
						Assert.assertEquals(i, val0.getDoubleValue(), 0.0000001);
						Assert.assertTrue(val1.isMissing());
					}
				}
			}
		}
	}

	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		// Read/Write table...
		try (final ArrowTable table = ArrowUtils.createArrowTable(BATCH_SIZE, OFFHEAP_SIZE, SCHEMAS)) {

			try (final WritableRow row = ColumnBackedWritableRow.fromWritableTable(table)) {
				final WritableDoubleValueAccess val0 = (WritableDoubleValueAccess) row.getValueAccessAt(0);
				final WritableDoubleValueAccess val1 = (WritableDoubleValueAccess) row.getValueAccessAt(1);
				for (long i = 0; i < NUM_ROWS; i++) {
					row.fwd();
					if (i % NUM_ROWS == 0) {
						val0.setMissing();
						val1.setDoubleValue(i);
					} else {
						val0.setDoubleValue(i);
						val1.setMissing();
					}
				}
			}

			try (final ReadableRow row = ColumnBackedReadableRow.fromReadableTable(table)) {
				final ReadableDoubleValueAccess val0 = (ReadableDoubleValueAccess) row.getValueAccessAt(0);
				final ReadableDoubleValueAccess val1 = (ReadableDoubleValueAccess) row.getValueAccessAt(1);
				for (long i = 0; row.canFwd(); i++) {
					row.fwd();
					if (i % NUM_ROWS == 0) {
						Assert.assertTrue(val0.isMissing());
						Assert.assertEquals(i, val1.getDoubleValue(), 0.0000001);
					} else {
						Assert.assertEquals(i, val0.getDoubleValue(), 0.0000001);
						Assert.assertTrue(val1.isMissing());
					}
				}
			}
		}
	}
}
/*
 * We can revisit this later. we're nearly done with an implementation which is
 * also suitable for streaming :-)
 */
//	@Test
//	public void readWhileWriteTest() throws Exception {
//
//		long NUM_ROWS = 100000;
//
//		// Read/Write table...
//		try (final ArrowStore store = createStore(NUM_ROWS);
//				final CachedColumnPartitionedTable table = new CachedColumnPartitionedTable(
//						new ColumnSchema[] { doubleVectorSchema }, store)) {
//
//			final Thread t1 = new Thread("Producer") {
//				public void run() {
//					// read AND write...
//					try (final WritableColumn column = table.getWritableColumn(0)) {
//						final WritableDoubleValueAccess value = (WritableDoubleValueAccess) column.getValueAccess();
//						for (long i = 0; i < NUM_ROWS; i++) {
//							column.fwd();
//							if (i % NUM_ROWS / 100 == 0) {
//								value.setMissing();
//							} else {
//								value.setDoubleValue(i);
//							}
//						}
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				};
//			};
//
//			final Thread t2 = new Thread("Producer") {
//				public void run() {
//					// then read
//					try (final ReadableColumnCursor readableColumn = table.createReadableColumnCursor(0)) {
//						final ReadableDoubleValueAccess readableValue = (ReadableDoubleValueAccess) readableColumn
//								.getValueAccess();
//						for (long i = 0; readableColumn.canFwd(); i++) {
//							readableColumn.fwd();
//							if (i % NUM_ROWS / 100 == 0) {
//								Assert.assertTrue(readableValue.isMissing());
//							} else {
//								System.out.println(readableValue.getDoubleValue());
//								Assert.assertEquals(i, readableValue.getDoubleValue(), 0.0000001);
//							}
//						}
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				};
//			};
//
//			t1.run();
//			t2.run();
//
//		}
//	}

/*
 *
 * MOCKS TO MIMIC KNIME API
 *
 */
//	@Test
//	public void pushViaKNIMEAPI() throws IOException {
////		final DataTableSpec spec = null;
////		final DataContainer container = new DataContainer() {
////
////			private final WritableTable m_table;
////			{
////				final Store store = new ArrowStoreNewestOld(convert(spec));
////				m_table = new DefaultWritableTable(store);
////			}
////
////			private ColumnSchema[] convert(final DataTableSpec spec) {
////				return null;
////			}
////
////			@Override
////			public void addRowToTable(final DataRow row) {
////			}
////
////			@Override
////			public DataTableSpec getSpec() {
////				return null;
////			}
////		};
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
//}
