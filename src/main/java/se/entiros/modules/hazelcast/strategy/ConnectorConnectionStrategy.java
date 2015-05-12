/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package se.entiros.modules.hazelcast.strategy;

import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.param.Default;

/**
 * Configuration type Strategy
 *
 * @author MuleSoft, Inc.
 */
@Configuration(configElementName = "config-type", friendlyName = "Configuration type strategy")
public class ConnectorConnectionStrategy
{
    /**
     * Configurable
     */
    @Configurable
    @Default("value")
    private String myStrategyProperty;

    /**
     * Set strategy property
     *
     * @param myStrategyProperty my strategy property
     */
    public void setMyStrategyProperty(String myStrategyProperty) {
        this.myStrategyProperty = myStrategyProperty;
    }

    /**
     * Get property
     */
    public String getMyStrategyProperty() {
        return this.myStrategyProperty;
    }

}