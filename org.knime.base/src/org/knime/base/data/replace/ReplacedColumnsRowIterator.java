/* 
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 * 
 */
package org.knime.base.data.replace;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;

/**
 * 
 * @author Bernd Wiswedel, University of Konstanz
 */
public class ReplacedColumnsRowIterator extends RowIterator {
    private final RowIterator m_it;

    private final ReplacedCellsFactory m_cellFactory;

    private final DataType[] m_validateTypes;

    private final int[] m_columns;

    /**
     * Creates a new replaced column iterator.
     * 
     * @param it the iterator in which one or more columns are replaced
     * @param fac the factory for the replacement cells
     * @param validateTypes the new data types
     * @param columns the column indices to replace
     */
    ReplacedColumnsRowIterator(final RowIterator it,
            final ReplacedCellsFactory fac, final DataType[] validateTypes,
            final int[] columns) {
        m_it = it;
        m_cellFactory = fac;
        m_validateTypes = validateTypes;
        m_columns = columns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return m_it.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataRow next() {
        DataRow origNext = m_it.next();
        DataCell[] newCells = m_cellFactory.getReplacement(origNext, m_columns);
        for (int i = 0; i < m_columns.length; i++) {
            if (!m_validateTypes[i].isASuperTypeOf(newCells[i].getType())) {
                // !m_validateClass.isAssignableFrom(newCell.getClass())) {
                // TODO: Check if correct
                throw new IllegalStateException(
                        "Cell generated by factory ( \"" + newCells[i]
                                + "\") is not subclass of column class: "
                                + m_validateTypes[i].getClass().getName()
                                + " vs. " + newCells[i].getClass());
            }
        }
        return new ReplacedColumnsDataRow(origNext, newCells, m_columns);
    }
}
