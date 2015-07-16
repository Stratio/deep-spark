package com.stratio.deep.cassandra.util;

import com.stratio.deep.commons.exception.DeepExtractorInitializationException;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * This class must manage inetAddress.
 * Created by jmgomez on 14/07/15.
 */
public class InetAddressConverter {

    private static final Logger LOG = Logger.getLogger(InetAddressConverter.class);

    /**
     * Transform a list of host to a list of InetAdress
     * @param hosts a comma separate host list.
     * @return a inetAddress collection.
     */
    public static Collection<InetAddress> toInetAddress(String hosts) throws DeepExtractorInitializationException {
        Collection<InetAddress> returnValue = Collections.EMPTY_LIST;
        LOG.info("Turn "+hosts+"into InetAddress");
        try {
            String[] hostList = hosts.split(",");
            returnValue = new ArrayList<>(hostList.length);
            for (String address: hostList){
                returnValue.add(InetAddress.getByName(address));
            }
        } catch (UnknownHostException e) {
            String msg = "Error create inetAddress from "+hosts+"."+e.getMessage();
            LOG.error(msg);
            throw new DeepExtractorInitializationException(msg,e);
        }

        return returnValue;
    }
}
