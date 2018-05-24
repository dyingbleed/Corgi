package com.dyingbleed.corgi.web.controller;

import com.dyingbleed.corgi.web.controller.api.DataSourceController;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by 李震 on 2018/5/9.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebMvcTest(DataSourceController.class)
public class DataSourceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void insertDataSourceTest() throws Exception {
        this.mockMvc.perform(
                        put("/datasource")
                            .param("name", "test")
                            .param("url", "test")
                            .param("username", "test")
                            .param("password", "test")
                            .accept(MediaType.APPLICATION_JSON)
         ).andExpect(status().isOk()).andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void queryAllDataSourceTest() throws Exception {
        this.mockMvc.perform(
                        get("/datasource")
                            .accept(MediaType.APPLICATION_JSON)
        ).andExpect(status().isOk()).andDo(MockMvcResultHandlers.print());
    }

}
