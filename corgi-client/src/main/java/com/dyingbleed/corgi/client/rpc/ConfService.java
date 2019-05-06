package com.dyingbleed.corgi.client.rpc;

import com.dyingbleed.corgi.core.bean.DMTask;
import com.dyingbleed.corgi.core.bean.ODSTask;
import retrofit2.http.GET;
import retrofit2.http.Query;

/**
 * Created by 李震 on 2019-05-05.
 */
public interface ConfService {

    @GET("/api/v1/conf/ods")
    ODSTask getODSTaskConf(@Query("name")String name);

    @GET("/api/v1/conf/dm")
    DMTask getDMTaskConf(@Query("name")String name);

}
