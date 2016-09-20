package com.epam.bigdata.hive;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * Created by Ilya_Starushchanka on 9/20/2016.
 */
public class UserAgentUDTF extends GenericUDTF {

    private Object[] fwdObj = null;
    private PrimitiveObjectInspector userAgentDtlOI = null;

    @Override
    public StructObjectInspector initialize (ObjectInspector[] arg){
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        userAgentDtlOI = (PrimitiveObjectInspector) arg[0];
        fieldNames.add("UA_type");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
        fieldNames.add("UA_family");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
        fieldNames.add("OS_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
        fieldNames.add("Device");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
        fwdObj = new Object[4];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        final String userAgentString = userAgentDtlOI.getPrimitiveJavaObject(objects[0]).toString();
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
        fwdObj[0] = userAgent.getBrowser().getBrowserType().getName();
        fwdObj[1] = userAgent.getBrowser().getGroup().getName();
        fwdObj[2] = userAgent.getOperatingSystem().getName();
        fwdObj[3] = userAgent.getOperatingSystem().getDeviceType().getName();
        forward(fwdObj);
    }

    @Override
    public void close() throws HiveException {
        forward(fwdObj);
    }
}
