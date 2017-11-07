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
 */
package org.knime.base.data.aggregation.dialogutil;

import java.awt.Color;
import java.awt.Component;
import java.awt.event.MouseEvent;

import javax.swing.BorderFactory;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

/**
 * Table cell renderer that handles rendering of invalid {@link AggregationFunctionRow}s. The implementation checks
 * if the value being renderer is of type {@link AggregationFunctionRow} if so it uses the given
 * {@link ValueRenderer} implementation to render a specific value. If not, the passed value's toString() method is
 * used for rendering.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 * @param <R> the {@link AggregationFunctionRow}
 * @see AggregationFunctionAndRowTableCellRenderer
 * @since 2.11
 */
public class AggregationFunctionRowTableCellRenderer<R extends AggregationFunctionRow<?>>
extends DefaultTableCellRenderer {
    private static final long serialVersionUID = 1L;

    private final ValueRenderer<R> m_renderer;

    private final boolean m_checkValidFlag;

    private final String m_toolTip;

    /** Used to render a specific value of an {@link AggregationFunctionRow}.
     * @param <R> the {@link AggregationFunctionRow}*/
    public interface ValueRenderer<R> {
        /**
         * @param c the {@link DefaultTableCellRenderer} to display the value
         * @param row the {@link AggregationFunctionRow} to display the value for
         */
        public void renderComponent(final DefaultTableCellRenderer c, final R row);
    }

    /**
     * @param valueRenderer the {@link ValueRenderer} to use
     */
    public AggregationFunctionRowTableCellRenderer(final ValueRenderer<R> valueRenderer) {
        this(valueRenderer, true);
    }

    /**
     * @param valueRenderer the {@link ValueRenderer} to use
     * @param checkValidFlag <code>true</code> if the valid flag of the {@link AggregationFunctionRow}
     * should be checked
     */
    public AggregationFunctionRowTableCellRenderer(final ValueRenderer<R> valueRenderer,
        final boolean checkValidFlag) {
        this(valueRenderer, checkValidFlag, "Left mouse click to change method. Right mouse click for context menu.");
    }
    /**
     * @param valueRenderer the {@link ValueRenderer} to use
     * @param checkValidFlag <code>true</code> if the valid flag of the {@link AggregationFunctionRow}
     * should be checked
     * @param toolTip the tool tip to show or <code>null</code> for none
     * @since 2.11
     */
    public AggregationFunctionRowTableCellRenderer(final ValueRenderer<R> valueRenderer,
        final boolean checkValidFlag, final String toolTip) {
        if (valueRenderer == null) {
            throw new NullPointerException("renderer must not be null");
        }
        m_renderer = valueRenderer;
        m_checkValidFlag = checkValidFlag;
        m_toolTip = toolTip;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Component getTableCellRendererComponent(final JTable table, final Object value, final boolean isSelected,
                                                   final boolean hasFocus, final int rowIdx, final int column) {
        final Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, rowIdx, column);
        assert (c == this);
        if (value instanceof AggregationFunctionRow) {
            @SuppressWarnings("unchecked")
            final R row = (R)value;
            m_renderer.renderComponent(this, row);
            if (m_checkValidFlag && !row.isValid()) {
                //set a red border for invalid methods
                setBorder(BorderFactory.createLineBorder(Color.RED));
            } else {
                setBorder(null);
            }
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getToolTipText() {
        return m_toolTip;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getToolTipText(final MouseEvent event) {
        return getToolTipText();
    }
}
