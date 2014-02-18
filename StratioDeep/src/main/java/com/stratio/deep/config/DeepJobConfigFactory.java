package com.stratio.deep.config;

import java.io.Serializable;

import com.stratio.deep.config.impl.CellDeepJobConfig;
import com.stratio.deep.config.impl.EntityDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;

/**
 * Factory class for deep configuration objects.
 * 
 * @author Luca Rosellini <luca@stratio.com>
 *
 */
public final class DeepJobConfigFactory implements Serializable {

    private static final long serialVersionUID = -4559130919203819088L;

    /**
     * Creates a new cell-based job configuration object.
     * @return
     */
    public static IDeepJobConfig<Cells> create() {
	return new CellDeepJobConfig();
    }

    /**
     * Creates an entity-based configuration object.
     * 
     * @return
     */
    public static <T extends IDeepType> IDeepJobConfig<T> create(Class<T> entityClass) {
	return new EntityDeepJobConfig<>(entityClass);
    }

    /**
     * Private constructor.
     */
    private DeepJobConfigFactory() {
    }
}
