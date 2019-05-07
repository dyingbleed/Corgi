$(function () {
    // 数据对象
    var logArray = [];

    var codeList = {
        "success": "成功",
        "failed": "失败"
    };

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            logArray: logArray
        },
        methods: {
            stateClass: function (i) {
                if (i.state === 'failed') {
                    return "table-danger";
                } else {
                    return "table-info";
                }
            },
            stateName: function (state) {
                return codeList[state];
            }
        }
    });

    /**
     * 查询所有告警
     * */
    function queryLogById(id) {
        var url = '/api/ods/log/' + id
        $.get(url).done(function (data) {
            app.logArray = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    if (!_.isNull(id)) {
        queryLogById(id);
    }
});