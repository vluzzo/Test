package com.sirius.plugins;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class PGISPluginData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta;
	
    public PGISPluginData()
	{
		super();
	}
}
	
