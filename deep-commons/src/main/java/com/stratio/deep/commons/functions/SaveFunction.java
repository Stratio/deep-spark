/**
 * 
 */
package com.stratio.deep.commons.functions;


import com.stratio.deep.commons.entity.Cells;

import java.io.Serializable;

/**
 *
 */
public interface SaveFunction extends Serializable{

    public void call(Cells cells);
}
