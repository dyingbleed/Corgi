package com.dyingbleed.corgi.client.rpc;

import retrofit2.http.POST;
import retrofit2.http.Query;

/**
 * Created by 李震 on 2019-05-05.
 */
public interface RunService {

    @POST("/api/v1/run/ods")
    void runODSTask(@Query("name")String name);

    @POST("/api/v1/run/dm")
    void runDMTask(@Query("name")String name);

}
