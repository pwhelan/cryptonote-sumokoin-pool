require('../lib/configReader.js');

//var jsonrpc = require('multitransport-jsonrpc'); // Get the multitransport JSON-RPC suite
//var Server = jsonrpc.server; // The server constructor function
//var ServerTcp = jsonrpc.transports.server.tcp; // The server TCP transport constructor function


global.config.daemon = {
    "host": "127.0.0.1",
    "port": 8081
};

global.config.wallet =  {
    "host": "127.0.0.1",
    "port": 8081
};

global.config.api = {
    "enabled": true,
    "hashrateWindow": 600,
    "updateInterval": 20,
    "port": 8081,
    "blocks": 30,
    "payments": 30,
    "password": "your_password",
    "trust_proxy_ip": false
};

var dateFormat = require('dateformat');
var util = require("util");

global.log = function(severity, system, text, data) {
    
    var time = dateFormat(new Date(), 'yyyy-mm-dd HH:MM:ss');
    var formattedMessage = text;
    
    if (severity == "info") return;
    
    if (data) {
        data.unshift(text);
        formattedMessage = util.format.apply(null, data);
    }

    //console.log(time + ' [' + system + '] ' + formattedMessage);
};


var async = require('async');
var assert = require('assert');
var rewire = require('rewire');
var redis = require("redis-js");

var paymentProcessor = rewire('../lib/paymentProcessor.js');

redisClient = global.redisClient = redis.createClient();

describe('paymentProcessor', function() {
    var getWorkers = paymentProcessor.__get__("getWorkers");
    var getWorkerBalances = paymentProcessor.__get__("getWorkerBalances");
    var filterBalances = paymentProcessor.__get__("filterBalances");
    var payoutPayments = paymentProcessor.__get__("payoutPayments");
    
    describe("getWorkers:5", function() {
        it("has 5 workers", function() {
            return Promise.all([
                redisClient.hset(config.coin + ":workers:worker1", "balance", 1),
                redisClient.hset(config.coin + ":workers:worker2", "balance", 2),
                redisClient.hset(config.coin + ":workers:worker3", "balance", 3),
                redisClient.hset(config.coin + ":workers:worker4", "balance", 4),
                redisClient.hset(config.coin + ":workers:worker5", "balance", 5)
            ])
            .then(function() {
                var workercount = 0;
                
                return new Promise(function(resolve, reject) {
                    paymentProcessor.__get__("getWorkers")(function(err, workers) {
                        if (err === true) {
                            Promise.all(workers).then(function() {
                                resolve(workercount);
                            });
                            return;
                        } else if (err) {
                            Promise.all(workers).then(reject);
                            return;
                        }
                        workers.forEach(function(worker) {
                            workercount++;
                            worker.resolve();
                        });
                    });
                }).then(function(workercount) {
                    assert.equal(workercount, 5);
                }).catch(function() {
                    assert.fail("error");
                });
            });
        });
    });
    
    describe("getWorkers:95", function() {
        it("has 95 workers", function() {
            var ps = [];
            
            for (i = 0; i < 95; i++) {
                ps.push(redisClient.hset(
                    config.coin + ":workers:worker" + i,
                    "balance", i * 1000000000
                ));
            }
            
            var workercount = 0;
            return Promise.all(ps).then(function() {
                return new Promise(function(resolve, reject) {
                    paymentProcessor.__get__("getWorkers")(function(err, workers) {
                        if (err === true) {
                            Promise.all(workers).then(function() {
                                resolve(workers, workercount);
                            });
                            return;
                        } else if (err) {
                            Promise.all(workers).then(reject);
                            return;
                        }
                        workercount += workers.length;
                        assert.ok(workers.length < config.payments.maxAddresses+10);
                        workers.forEach(function(worker) {
                            worker.resolve(worker);
                        });
                    });
                });
            }).then(function(workers) {
                assert.equal(workers.length, 95);
                assert.equal(workercount, 95);
            }).catch(function(err) {
                assert.fail(err);
            });
        })
    });
    
    describe("getWorkerBalances", function() {
        it("get all worker balances", function() {
            
            return new Promise(function(resolve, reject) {
                async.waterfall([
                    getWorkers,
                    getWorkerBalances,
                    function(workers, callback) {
                        workers.forEach(function(worker) {
                            assert.ok(parseInt(worker.address.substr(6)) * 1000000000 == worker.balance);
                            worker.resolve();
                        });
                    }
                ], function(err, promises) {
                    if (err === true) {
                        Promise.all(promises).then(function() {
                            resolve();
                        });
                        return;
                    } else if (err) {
                        Promise.all(promises).then(reject);
                        return;
                    }
                });
            });
        });
    });
    
    describe("filterBalances", function() {
        it("filter worker balances", function() {
            
            return new Promise(function(resolve, reject) {
                async.waterfall([
                    getWorkers,
                    getWorkerBalances,
                    filterBalances,
                    function(workers, callback) {
                        assert.ok(workers.length > 0);
                        workers.forEach(function(worker) {
                            assert.ok(worker.balance >= worker.minPayoutLevel);
                            worker.resolve();
                        });
                    }
                ], function(err, promises) {
                    if (err === true) {
                        Promise.all(promises).then(function() {
                            resolve();
                        });
                        return;
                    } else if (err) {
                        Promise.all(promises).then(reject);
                        return;
                    }
                });
            });
        });
    });
    
    describe("payoutPayments", function() {
        const crypto = require('crypto');
        const secret = 'abcdefg';
        
        var jayson = require('jayson');
        var server = jayson.server({
            transfer: function(args, callback) {
                //console.log("TRANSFER!");
                //console.log(args);
                const hash = crypto.createHmac('sha256', secret)
                       .update(Date.now() + "")
                       .digest('hex');
                
                args.destinations.forEach(function(dest) {
                    assert.ok(dest.amount > 0);
                });
                
                callback(null, {
                    tx_hash: "<" + hash + ">"
                });
            }
        });
        
        var http = server.http().listen(8081, function() {
            //console.log("listening!");
        });
        
        it("payout payments to workers", function() {
            return new Promise(function(resolve, reject) {
                async.waterfall([
                    getWorkers,
                    getWorkerBalances,
                    filterBalances,
                    payoutPayments
                ], function(err, promises) {
                    if (err === true) {
                        Promise.all(promises).then(function() {
                            resolve();
                        });
                        return;
                    } else if (err) {
                        Promise.all(promises).then(reject);
                        return;
                    }
                });
            });
        });
        
        after(function() {
            http.close();
        });
    });
});
