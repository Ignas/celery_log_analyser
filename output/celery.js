$(function () {

    var plot = function (options) {
        $.plot($("#started-finished"), [{ label: "Finished", data: finished }, { label: "Started", data: started }, { label: "Failed", data: failed }], options);
        $.plot($("#difference"), [ { label: "Running", data: total }, { label: "Difference", data: difference }], options);
        $.plot($("#time-avg"), [ { label: "Time", data: time_spent_avg }, { label: "Lag", data: lag_avg } ], options);
        $.plot($("#time-sum"), [ { label: "Time", data: time_spent }, { label: "Lag", data: lag } ], options);
        $.plot($("#time-max"), [ { label: "Time", data: time_spent_max }, { label: "Lag", data: lag_max } ], options);
    };

    $("#whole").click(function () {
        plot({ xaxis: { mode: "time" } });
    });

    var last = started[started.length - 1][0];
    $("#last-hour").click(function () {
        plot({
            xaxis: {
                mode: "time",
                min: last - 3000000,
                max: last
            }
        });
    });

    $("#last-day").click(function () {
        plot({
            xaxis: {
                mode: "time",
                min: last - 24 * 3000000,
                max: last
            }
        });
    });
    plot({ xaxis: { mode: "time" } });
});
