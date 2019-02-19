$(function () {
    // 数据对象
    var alertArray = [];

    var codeList = {
        "SCHEMA_NOT_MATCH": "表结构不匹配"
    };

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            alertArray: alertArray
        },
        methods: {
            levelClass: function (i) {
                if (i.level === 0) {
                    return "table-danger";
                } else if (i.level === 1) {
                    return "table-warning";
                } else {
                    return "table-info";
                }
            },
            typeName: function (type) {
                return codeList[type];
            }
        }
    });

    function queryAllAlerts() {
        $.get('/api/detector').done(function (data) {
            app.alertArray = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    queryAllAlerts();
});