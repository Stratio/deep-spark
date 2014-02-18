package com.stratio.deep.config.impl;

import com.stratio.deep.entity.Cells;

/**
 * Cell-based configuration object.
 *
 * @author Luca Rosellini <luca@stratio.com>
 *
 */
public class CellDeepJobConfig extends GenericDeepJobConfig<Cells> {

    private static final long serialVersionUID = -598862509865396541L;
    private Cells dummyCells;

    {
	dummyCells = new Cells();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Cells> getEntityClass() {
	return (Class<Cells>) dummyCells.getClass();
    }

}
