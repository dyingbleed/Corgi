package com.dyingbleed.corgi.client.rpc;

import retrofit2.Call;
import retrofit2.http.POST;
import retrofit2.http.Query;

/**
 * Created by 李震 on 2019-05-05.
 */
public interface RunService {

    @POST("/api/v1/run/ods")
    Call<Void> runODSTask(@Query("name")String name);

    @POST("/api/v1/run/dm")
    Call<Void> runDMTask(@Query("name")String name);

}
