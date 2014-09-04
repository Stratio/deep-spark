/**
 * 
 */
package com.stratio.deep;

import com.stratio.deep.extractor.server.ExtractorServer;

/**
 * @author Óscar Puertas
 * 
 */
public final class Runner {

  public static void main(String[] args) throws Exception {

    try {
      ExtractorServer.start();

    } finally {
      ExtractorServer.close();
    }
  }
}
