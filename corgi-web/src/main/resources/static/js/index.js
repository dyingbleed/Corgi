function formatTime(time) {
    return moment(time).format('YYYY-MM-DD HH:MM:ss');
}

function formatSeconds(seconds) {
    if (seconds > 3600) {
        var hour = Math.floor(seconds / 3600);
        var minute = Math.floor(seconds % 3600 / 60);
        var second = seconds % 60;
        return hour + " 小时 " + minute + " 分钟 " + second + " 秒";
    } else if (seconds > 60) {
        var minute = Math.floor(seconds % 3600 / 60);
        var second = seconds % 60;
        return minute + " 分钟 " + second + " 秒";
    } else {
        return seconds + " 秒";
    }
}

function formatBytes(bytes) {
    if (bytes > (1024 * 1024 * 1024)) {
        var gb = Math.floor(bytes / (1024 * 1024 * 1024));
        var mb = Math.round(bytes % (1024 * 1024 * 1024) / (1024 * 1024));
        return gb + " GB " + mb + " MB";
    } else if (bytes > (1024 * 1024)) {
        var mb = Math.round(bytes / (1024 * 1024));
        return mb + " MB";
    } else if (bytes > 1024) {
        var kb = Math.round(bytes / 1024);
        return kb + " MB";
    } else {
        return bytes + " B";
    }
}

$(function () {
    // 数据对象
    var measureStat = {};
    var measureDetailArray = [];

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            measureStat: measureStat,
            measureDetailArray: measureDetailArray
        },
        method: {

        }
    });

    /**
     * 查询任务指标
     * */
    function queryTodayMeasureStat() {
        $.get('/api/measure/stat').done(function (data) {
            app.measureStat = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    /**
     * 查询任务明细
     * */
    function queryTodayMeasureDetail() {
        $.get('/api/measure/detail').done(function (data) {
            app.measureDetailArray = data;
        }).fail(function () {
            alert("查询失败！");
        });
    }

    queryTodayMeasureStat();
    queryTodayMeasureDetail();
});