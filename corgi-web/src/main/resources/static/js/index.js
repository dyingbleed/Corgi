$(function () {
    // 数据对象
    var scheduledTaskArray = [];
    var task = {};
    var batchTaskArray = [];

    // Vue 实例
    var app = new Vue({
        el: '#app',
        data: {
            scheduledTaskArray: scheduledTaskArray,
            task: task,
            batchTaskArray: batchTaskArray
        },
        methods: {
            scheduleBatchTask: function () {
                scheduleBatchTask(app.task);
                getAllScheduledTask();
            },
            unscheduleBatchTask: function (id) {
                unscheduleBatchTask(id);
            }
        }
    });

    $('#scheduledTaskModal').on('shown.bs.modal', function (e) {
        getAllBatchTask();
    });

    $('#scheduledTaskModal').on('hidden.bs.modal', function (e) {
        app.scheduleTask = {}
    });

    /**
     * 调度批处理任务
     *
     * @param task
     *
     * */
    function scheduleBatchTask(task) {
        var url = '/api/scheduler/' + task.batch_task_id;
        $.ajax(url, {
            method: 'PUT',
            data: task
        }).done(function () {
            $('#scheduledTaskModal').modal('hide');
            getAllScheduledTask();
        });
    }

    function unscheduleBatchTask(id) {
        var url = '/api/scheduler/' + id;
        $.ajax(url, {
            method: 'DELETE'
        }).done(function () {
            alert("删除成功！");
            getAllScheduledTask();
        });
    }

    /**
     * 获取所有的已调度任务
     * */
    function getAllScheduledTask() {
        $.get('/api/scheduler').done(function (data) {
            app.scheduledTaskArray = data;
        });
    }

    /**
     * 获取所有的批处理任务
     * */
    function getAllBatchTask() {
        $.get('/api/batch').done(function (data) {
            app.batchTaskArray = data;
        });
    }

    getAllScheduledTask();
});