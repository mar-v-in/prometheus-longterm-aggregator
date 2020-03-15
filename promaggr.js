/**
 * Database schema
 * metrics: id(int32),name(string),aggr(string),end_of_fetch
 * data: metric(int32),timestamp(long/int64),value(double/float64)
 */


// Includes

const _ = require('lodash')
const mysql = require('mysql2')
const http = require('http')
const url = require('url')
const cluster = require('cluster')

// load configuration

let argv = require('minimist')(process.argv.slice(2))
argv.config = _.get(argv, 'config', './config.json')

let config = JSON.parse(require('fs').readFileSync(argv.config, 'utf8'))


// general purpose code

function logError(err) {
    if (err) {
        console.log(err.stack)
        return true
    }
    return false
}

// database code

let connection_pool = null
let connection = null
if (_.get(config, "db.type") == "mysql") {
    connection_pool = mysql.createPool(config.db.config);
    //connection = mysql.createConnection(config.db.config)
    //connection.connect((err) => logError(err))
} else {
    console.log("!! No database backend defined.")
    process.exit()
}

function execute(sql, values, cb) {
    if (connection_pool != null) {
        connection_pool.execute(sql, values, cb)
    }
    if (connection != null) {
        try {
            connection.execute(sql, values, cb)
        } catch (e) {
            connection.connect((err) => {
                cb(null, err)
            })
        }
    }
}

function query(sql, values, cb) {
    if (connection_pool != null) {
        connection_pool.query(sql, values, cb)
    }
    if (connection != null) {
        try {
            connection.query(sql, values, cb)
        } catch (e) {
            connection.connect((err) => {
                cb(null, err)
            })
        }
    }
}

function metricIdForNamed(metricName, aggr, callback) {
    execute("SELECT id FROM metrics WHERE name LIKE ? AND aggr LIKE ?", [metricName, aggr], (err, res) => {
        if (logError(err)) return
        if (res && res[0] && res[0].id) return callback(res[0].id)
        execute("INSERT INTO metrics (name, aggr) VALUES ( ? , ? ) ON DUPLICATE KEY UPDATE name=name", [metricName, aggr], (err, res) => {
            if (logError(err)) return
            callback(res.insertId)
        })
    })
}

function maxStampForMetric(metricName, callback) {
    execute("SELECT MAX(timestamp) AS maxstamp, MIN(end_of_fetch) AS end_of_fetch FROM metrics LEFT JOIN data ON data.metric = metrics.id WHERE metrics.name = ?", [metricName], (err, res) => {
        if (logError(err)) return
        callback(Math.max(res[0].maxstamp, res[0].end_of_fetch))
    })
}

function getDataForPostProcessing(metricName, aggr, startStamp, endStamp, callback, errorCallback) {
    let query = "SELECT data.timestamp AS timestamp, data.value AS value FROM metrics JOIN data ON data.metric = metrics.id WHERE metrics.name = ? AND metrics.aggr = ? AND data.timestamp >= ? AND data.timestamp <= ? ORDER BY timestamp ASC"
    let queryArgs = [metricName, aggr, startStamp, endStamp]
    execute(query, queryArgs, (err, res) => {
        if (logError(err)) return errorCallback()
        let results = [{metric: {__name__: aggr + "(" + metricName + ")"}, values: []}]
        res.forEach((row) => {
            results[0].values.push([row.timestamp, row.value + ""])
        })
        callback(results)
    })
}

function valueSelectFromAggr(aggr) {
    if (aggr == "avg" || aggr == "min" || aggr == "max") return aggr + "(data.value)"
    if (aggr.indexOf("q") === 0) {
        let q = parseFloat(aggr.substr(1, aggr.length))
        return "percentile_disc_compat(data.value, " + q + ")"
    }
    return null
}

function getDataLargeRangeAndStep(metricName, aggr, range, startStamp, endStamp, step, callback, errorCallback) {
    let valueSelect = valueSelectFromAggr(aggr)
    if (valueSelect == null) return errorCallback()
    let query = "SELECT MAX(data.timestamp) AS timestamp, " + valueSelect + " AS value FROM (SELECT DISTINCT metrics.id AS id, CEIL(data.timestamp / ?) AS timestamp_step FROM metrics JOIN data ON data.metric = metrics.id WHERE metrics.name = ? AND data.timestamp >= ? AND data.timestamp <= ? AND metrics.aggr = ?) AS sub JOIN data ON data.metric = sub.id AND data.timestamp > (sub.timestamp_step * ?) - ? AND data.timestamp <= sub.timestamp_step * ? GROUP BY sub.timestamp_step ORDER BY timestamp"
    let queryArgs = [step, metricName, startStamp, endStamp, aggr, step, range, step]
    execute(query, queryArgs, (err, res) => {
        if (logError(err)) return errorCallback()
        let results = [{metric: {__name__: aggr + "(" + metricName + "[" + range + "])"}, values: []}]
        res.forEach((row) => {
            results[0].values.push([row.timestamp, row.value + ""])
        })
        callback(results)
    })
}

function getDataEqualRangeAndStep(metricName, aggr, rangeAndStep, startStamp, endStamp, callback, errorCallback) {
    let valueSelect = valueSelectFromAggr(aggr)
    if (valueSelect == null) return errorCallback()
    let query = "SELECT MAX(data.timestamp) AS timestamp, " + valueSelect + " AS value FROM metrics JOIN data ON data.metric = metrics.id WHERE metrics.name = ? AND metrics.aggr = ? AND data.timestamp >= ? AND data.timestamp <= ? GROUP BY (CEIL(data.timestamp/?)) ORDER BY timestamp ASC"
    let queryArgs = [metricName, aggr, startStamp, endStamp, rangeAndStep]
    execute(query, queryArgs, (err, res) => {
        if (logError(err)) return errorCallback()
        let results = [{metric: {__name__: aggr + "(" + metricName + "[" + rangeAndStep + "])"}, values: []}]
        res.forEach((row) => {
            results[0].values.push([row.timestamp, row.value + ""])
        })
        callback(results)
    })
}

/**
 * @param data two dimensional array: [[timestamp, value],[timestamp, value],...]
 */
function storeInDatabase(metricName, aggr, data, cont) {
    if (data.length > 0) {
        metricIdForNamed(metricName, aggr, (metric_id) => {
            let minKey = data[0][0]
            let maxKey = data[0][0]
            let dbData = []
            data.forEach((entry) => {
                minKey = Math.min(minKey, entry[0])
                maxKey = Math.max(maxKey, entry[0])
                dbData.push([metric_id, entry[0], isNaN(parseFloat(entry[1])) ? null : parseFloat(entry[1])])
            })
            query("INSERT INTO data (metric, timestamp, value) VALUES ? ON DUPLICATE KEY UPDATE value=value", [dbData], (err, _) => {
                logError(err)
                if (cont) cont()
            })
        })
    }
}

function storeEndOfFetch(metricName, aggr, val, cont) {
    metricIdForNamed(metricName, aggr, (metric_id) => {
        query("UPDATE metrics SET end_of_fetch=? WHERE id=?", [val, metric_id], (err, _) => {
            logError(err)
            if (cont) cont()
        })
    })
}

// prometheus client code

function queryPrometheusHourly(metric, startStamp, cont) {
    let startHour = new Date(startStamp * 1000)
    startHour.setMinutes(0, 0, 0)
    startStamp = Math.floor(startHour.getTime() / 1000)
    if (startStamp < config.upstream.start) startStamp = config.upstream.start

    let endStamp = startStamp + 2591990 // 30 days - 10 secs
    let lastHour = new Date()
    lastHour.setMinutes(0, 0, 0)
    if (endStamp > (lastHour.getTime() / 1000)) endStamp = Math.floor(lastHour.getTime() / 1000)

    if (startStamp >= endStamp) {
        console.log("Metric is up-to-date")
        return cont()
    }

    const counterToCont = metric.overTimes.length + metric.quantiles.length
    let counter = 0

    function queryCont() {
        if (++counter == counterToCont && cont) cont(endStamp)
    }

    function handleQueryData(aggr) {
        return function (res) {
            let rawData = ''
            res.on('data', (chunk) => rawData += chunk)
            res.on('end', () => {
                try {
                    let parsedData = JSON.parse(rawData)
                    if (parsedData && parsedData.data && parsedData.data.result && parsedData.data.result[0] && parsedData.data.result[0].values)
                        storeInDatabase(metric.name, aggr, parsedData.data.result[0].values, queryCont)
                    else {
                        storeEndOfFetch(metric.name, aggr, endStamp, queryCont)
                    }
                } catch (e) {
                    console.log("!! " + e.message)
                }
            })
        }
    }

    metric.overTimes.forEach((type) => {
        let url = config.upstream.url + "api/v1/query_range?query=" + type + "_over_time(" + metric.query + ")&start=" + startStamp + "&end=" + endStamp + "&step=1h"
        http.get(url, handleQueryData(type)).on('error', (err) => {
            logError(err)
            if (queryCont) queryCont()
        })
    })
    metric.quantiles.forEach((quant) => {
        let url = config.upstream.url + "api/v1/query_range?query=quantile_over_time(" + quant + "," + metric.query + ")&start=" + startStamp + "&end=" + endStamp + "&step=1h"
        http.get(url, handleQueryData("q" + quant)).on('error', (err) => {
            logError(err)
            if (queryCont) queryCont()
        })
    })
}

function queryPrometheusManually(metric, startStamp, cont) {
    let startHour = new Date(startStamp * 1000)
    startHour.setMinutes(0, 0, 0)
    startStamp = Math.floor(startHour.getTime() / 1000)
    if (startStamp < config.upstream.start) startStamp = config.upstream.start

    let endStamp = startStamp + 172790 // 12 hours - 10 secs
    let lastHour = new Date()
    lastHour.setMinutes(0, 0, 0)
    if (endStamp >= (lastHour.getTime() / 1000)) endStamp = Math.floor(lastHour.getTime() / 1000) - 1

    if (startStamp >= endStamp) {
        console.log("Metric is up-to-date")
        return cont()
    }

    const manualAggrs = metric.quantiles.map((q) => "q" + q)
    metric.overTimes.forEach((t) => manualAggrs.push(t))

    const counterToCont = manualAggrs.length
    let counter = 0

    function queryCont() {
        if (++counter == counterToCont && cont) cont(endStamp)
    }

    function handleQueryData(res) {
        let rawData = ''
        res.on('data', (chunk) => rawData += chunk)
        res.on('end', () => {
            try {
                let parsedData = JSON.parse(rawData)
                if (parsedData && parsedData.data && parsedData.data.result && parsedData.data.result[0] && parsedData.data.result[0].values) {
                    let dataByHour = []
                    let thisStamp = 0
                    parsedData.data.result[0].values.forEach((pair) => {
                        if (pair[0] >= thisStamp + 60) {
                            thisStamp = pair[0]
                            let thisHour = new Date(thisStamp * 1000)
                            thisHour.setMinutes(0, 0, 0)
                            thisStamp = thisHour.getTime() / 1000
                            dataByHour.push([thisStamp, [pair[1]]])
                        } else {
                            dataByHour[dataByHour.length - 1][1].push(parseFloat(pair[1]))
                        }
                    })
                    manualAggrs.forEach((aggr) => {
                        let metrics = []
                        dataByHour.forEach((pair) => {
                            metrics.push([pair[0], aggregate(aggr, pair[1])])
                        })
                        storeInDatabase(metric.name, aggr, metrics, queryCont)
                    })
                } else {
                    manualAggrs.forEach((aggr) => {
                        storeEndOfFetch(metric.name, aggr, endStamp, queryCont)
                    })
                }
            } catch (e) {
                logError(e)
            }
        })
    }

    let url = config.upstream.url + "api/v1/query_range?query=" + metric.query + "&start=" + startStamp + "&end=" + endStamp + "&step=1m"
    http.get(url, handleQueryData).on('error', (err) => {
        logError(err)
        if (cont) cont()
    })
}

function queryPrometheus(metric, startStamp, cont) {

    if (metric.mode == "hourly_req") {
        queryPrometheusHourly(metric, startStamp, cont)
    } else if (metric.mode == "manually_aggr") {
        queryPrometheusManually(metric, startStamp, cont)
    } else {
        console.log("!! Can't handle metric mode: " + metric.mode)
        if (cont) cont()
    }
}

function updateUpsteam(metric, cont) {
    maxStampForMetric(metric.name, (maxStamp) => {
        let startStamp = maxStamp + 3600
        queryPrometheus(metric, startStamp, cont)
    })
}

function updateAll() {
    let currentMetric = 0
    const limit = 10
    let thisCount = 0

    function updateCont(lastStamp) {
        if (lastStamp && lastStamp < Date.now() - 2592000) {
            if (++thisCount == limit) {
                console.log("Metric not done (is at " + new Date(lastStamp * 1000) + " now), but limit of " + limit + " reached. will continue later")
                thisCount = 0
            } else {
                currentMetric--
            }
        } else {
            thisCount = 0
        }
        if (config.metrics[currentMetric]) {
            let metric = _.merge(config.metric_defaults, config.metrics[currentMetric++])
            if (thisCount == 0) {
                console.log("About to update metric: " + metric.name)
            }
            updateUpsteam(metric, updateCont)
        }
        else console.log("Metric update done")
    }

    updateCont()
}

function startUpstreamQuerying() {
    updateAll()
    setInterval(updateAll, 3600000)
}

// prometheus server code

function parseIntervalToSeconds(interval) {
    if (interval.endsWith("d")) return parseInt(interval) * 86400
    else if (interval.endsWith("h")) return parseInt(interval) * 3600
    else if (interval.endsWith("m")) return parseInt(interval) * 60
    else return parseInt(interval)
}

function parseOutQueryDetails(query) {
    let res = {metric: query, params: {}}
    if (query.indexOf("{") > 0) {
        res.metric = query.substr(0, query.indexOf("{"))
        let params = query.substr(query.indexOf("{") + 1, query.length - query.indexOf("{") - 2)
        params.split(",").forEach((param) => {
            let split = param.split("=")
            if (split.length != 2) throw new Error("label requires exactly one value")
            res.params[split[0]] = split[1]
        })
    }
    return res
}

function parseMyQuery(query, step) {
    let res = {
        metric: undefined,
        aggr: "avg",
        range: step
    }
    if (query.indexOf("(") > 0) {
        let methodName = query.substr(0, query.indexOf("("));
        query = query.substr(query.indexOf("(") + 1, query.length - query.indexOf("(") - 2);
        switch (methodName) {
            case "quantile_over_time":
                let split = query.split(",")
                let q = 0.5
                if (split.length == 2) {
                    q = parseFloat(split[1])
                    query = split[0]
                }
                res.aggr = "q" + q
                break
            case "avg_over_time":
                res.aggr = "avg"
                break
            case "min_over_time":
                res.aggr = "min"
                break
            case "max_over_time":
                res.aggr = "max"
                break
            case "stddev_over_time":
                res.aggr = "stddev"
                break
            default:
                return null
        }
    }
    if (query.indexOf("[") > 0) {
        let range = query.substr(query.indexOf("[") + 1, query.length - query.indexOf("[") - 2);
        query = query.substr(0, query.indexOf("["));
        res.range = parseIntervalToSeconds(range)
    }
    res.metric = query
    return res
}

function show500(stream, msg = "Internet server error") {
    console.log("ERR \"" + msg + "\" 500 - -")
    stream.writeHead(500, {'Content-Type': 'text/plain'})
    stream.write("500 " + msg)
    stream.end()
    return true
}

function aggregate(aggr, values) {
    function stddev(values) {
        let avg = average(values);

        let squareDiffs = values.map(function (value) {
            let diff = value - avg;
            return diff * diff;
        });

        let avgSquareDiff = average(squareDiffs);

        return Math.sqrt(avgSquareDiff);
    }

    function average(values) {
        let sum = values.reduce(function (sum, value) {
            return sum + value;
        }, 0);

        return sum / values.length;
    }

    if (!Array.isArray(values) || values.length == 0) return null;

    if (aggr.indexOf("q") === 0) {
        let q = parseFloat(aggr.substr(1, aggr.length))
        values.sort((a, b) => a - b)
        return values[Math.round(q * (values.length - 1))]
    }
    switch (aggr) {
        case "min":
            return Math.min.apply(Math, values)
        case "max":
            return Math.max.apply(Math, values)
        case "avg":
            return average(values)
        case "stddev":
            return stddev(values)
    }

    return values[values.length - 1]
}

function handleQueryRange(stream, raw_query, startStamp, endStamp, step) {
    let time_start = Date.now()

    step = parseIntervalToSeconds(step)
    let query = parseMyQuery(raw_query, step)

    function postProcess(result) {
        if (query.range > 3600) {
            let new_result = []
            result.forEach((entry) => {
                let new_entry = {metric: entry.metric, values: []}
                let active_selection = []
                entry.values.forEach((val) => {
                    while (active_selection.length > 0 && active_selection[0][0] + query.range <= val[0])
                        active_selection.shift()
                    active_selection.push(val)
                    new_entry.values.push([val[0], aggregate(query.aggr == "stddev" ? "avg" : query.aggr, active_selection.map((m) => parseFloat(m[1])))])
                })
                new_result.push(new_entry)
            })
            result = new_result
        }
        if (step > 3600) {
            let new_result = []

            result.forEach((entry) => {
                let new_entry = {metric: entry.metric, values: []}
                let last_val = -1
                entry.values.reverse()
                entry.values.forEach((val) => {
                    if (last_val == -1 || val[0] <= last_val - step) {
                        last_val = val[0]
                        new_entry.values.push(val)
                    }
                })
                new_entry.values.reverse()
                new_result.push(new_entry)
            })
            result = new_result
        }
        return result
    }

    function dataCallback(needPostProcessing, debugName) {
        return function (result) {
            let time_after_db = Date.now()

            if (needPostProcessing)
                result = postProcess(result)

            let res = {status: "success", "data": {resultType: "matrix", result: result}}

            console.log("GET \"" + raw_query + "\" 200 " + JSON.stringify(res).length + " " + (Date.now() - time_start))
            stream.writeHead(200, {'Content-Type': 'application/json'})
            stream.write(JSON.stringify(res))
            stream.end()
        }
    }

    function measureOnlyCallback(needPostProcessing, debugName) {
        return function (result) {
            let time_after_db = Date.now()

            if (needPostProcessing)
                result = postProcess(result)

            let res = {status: "success", "data": {resultType: "matrix", result: result}}

            time_start = Date.now()
            getDataForPostProcessing(query.metric, query.aggr, startStamp - Math.max(step, query.range), endStamp + Math.max(step, query.range), dataCallback(true, "using post processing"), () => show500(stream))
        }
    }

    if (query.aggr == "avg" || query.aggr == "min" || query.aggr == "max" || query.aggr.indexOf("q") === 0) {
        function euclid(a, b) {
            while (b != 0) {
                let t = a % b
                a = b
                b = t
            }
            return a
        }

        if (step == query.range) {
            // puts the effort of range/step into the database by using a group statement, very efficient, but required range == step
            getDataEqualRangeAndStep(query.metric, query.aggr, query.range, startStamp - Math.max(step, query.range), endStamp + Math.max(step, query.range), dataCallback(false, "equal range SQL"), () => show500(stream))
        } else if (euclid(step, query.range) > 3600 && Math.max(step, query.range) < euclid(step, query.range) * 168) {
            // let the db merge some data and do other by postprocessing
            getDataEqualRangeAndStep(query.metric, query.aggr, euclid(step, query.range), startStamp - Math.max(step, query.range), endStamp + Math.max(step, query.range), dataCallback(true, "combined SQL+pp"), () => show500(stream))
        } else if (step >= 86400 && query.range >= 604800) {
            // puts the effort of range/step into the database by using a subquery, only more efficient when using large range (like trend lines)
            getDataLargeRangeAndStep(query.metric, query.aggr, query.range, startStamp - Math.max(step, query.range), endStamp + Math.max(step, query.range), step, dataCallback(false, "large range SQL"), () => show500(stream))
        } else {
            // fallback to post processing only, it works everytime
            getDataForPostProcessing(query.metric, query.aggr, startStamp - Math.max(step, query.range), endStamp + Math.max(step, query.range), dataCallback(true, "fallback pp"), () => show500(stream))
        }
    } else {
        // get all required data and do range/step in js, not as inefficient as it sounds ;)
        getDataForPostProcessing(query.metric, query.aggr, startStamp - Math.max(step, query.range), endStamp + Math.max(step, query.range), dataCallback(true, "default pp"), () => show500(stream))
    }
    return true
}

function handleSeries(stream, match) {
    let res = {status: "success", data: []}
    match.forEach((m) => {
        let query = parseOutQueryDetails(m)
        if (query.params.aggr) {
            res.data.push({__name__: query.metric, aggr: query.params.aggr})
        } else {
            upstream_metrics.forEach((metric) => {
                if (m == metric.name) {
                    res.data.push({__name__: metric.name})
                }
            })
        }
    })
    stream.writeHead(200, {'Content-Type': 'application/json'})
    stream.write(JSON.stringify(res))
    stream.end()
    return true
}

/*
 function handleAggrLabelValues(stream) {
 let res = {status: "success", data: []}
 overTimes.forEach((aggr) => {
 res.data.push(aggr)
 })
 quantiles.forEach((quant) => {
 res.data.push("q" + quant)
 })
 stream.writeHead(200, {'Content-Type': 'application/json'})
 stream.write(JSON.stringify(res))
 stream.end()
 return true
 }*/

function handleNameLabelValues(stream) {
    let res = {status: "success", data: []}
    config.metrics.forEach((metric) => {
        res.data.push(metric.name)
    })
    stream.writeHead(200, {'Content-Type': 'application/json'})
    stream.write(JSON.stringify(res))
    stream.end()
    return true
}

http.createServer((req, stream) => {
    if (req.method != "GET") return show500(stream)
    try {
        let path = url.parse(req.url).pathname
        let params = url.parse(req.url, true).query
        switch (path) {
            case "/api/v1/query_range":
                if (!params.query) return show500(stream, "query is required")
                let query = params.query
                let start = parseInt(params.start) || Math.floor(Date.now() / 1000 - 3600)
                let end = parseInt(params.end) || Math.floor(Date.now() / 1000)
                let step = params.step || "1h"
                if (handleQueryRange(stream, query, start, end, step)) return
                return show500(stream, "Request not handled")
            case "/api/v1/series":
                let match = params["match[]"]
                if (!match) return show500(stream, "match[] is required")
                if (!Array.isArray(match)) match = [match]
                if (handleSeries(stream, match)) return
                return show500(stream, "Request not handled")
            /*            case "/api/v1/label/aggr/values":
             if (handleAggrLabelValues(stream)) return
             return show500(stream, "Request not handled")*/
            case "/api/v1/label/__name__/values":
                if (handleNameLabelValues(stream)) return
                return show500(stream, "Request not handled")
            default:
                return show500(stream, "Unknown request")
        }
    } catch (e) {
        show500(stream, "Internal server error: " + e.stack)
    }
}).listen(9123, '127.0.0.1', () => {
    console.log("Started prometheus-like interface on 127.0.0.1:9123")
})

startUpstreamQuerying()
