var fs = require('fs');

var async = require('async');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);


var logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);
var emailSystem = require('./email.js');


log('info', logSystem, 'Started');

// Get workers
function getWorkers(callback) {
    function getWorkersScan(cursor) {
        // TODO: add the count here. That way we *are* doing payments in lots
        // of X workers.
        redisClient.sscan(config.coin + ':workers', cursor, "COUNT", config.payments.maxAddresses, function(error, reply) {
            var cursor = parseInt(reply[0]);
            var workers = reply[1];
            
            console.log("WORKERS:");
            console.log(workers);
            callback(null, workers);
            if (cursor)
            {
                getWorkersScan(cursor);
            }
        });
    }
    getWorkersScan(0);
}

// Get balances for workers
function getWorkerBalances(workers, callback) {
    // get all the balances
    var redisCommands = workers.map(function(k){
        return ['hmget', config.coin + ':workers:' + k, 'balance', 'minPayoutLevel'];
    });
    
    redisClient.multi(redisCommands).exec(function(error, replies){
        if (error){
            log('error', logSystem, 'Error with getting balances from redis %j', [error]);
            callback(true);
            return;
        }
        var balances = {};
        for (var i = 0; i < replies.length; i++){
            var workerId = workers[i];
            balances[workerId] = parseInt(replies[i]) || 0
        }
        var balances = {};
        var minPayoutLevel = {};
        for (var i = 0; i < replies.length; i++){
            var workerId = workers[i];
            data = replies[i];
            balances[workerId] = parseInt(data[0]) || 0
            minPayoutLevel[workerId] = parseInt(data[1]) || config.payments.minPayment
            log('info', logSystem, 'Using payout level %d for worker %s (default: %d)', [minPayoutLevel[workerId], workerId, config.payments.minPayment]);
        }
        console.log("BALANCES:");
        console.log(balances);
        callback(null, balances, minPayoutLevel);
    });
}
        
//Filter workers under balance threshold for payment
function filterBalances(balances, minPayoutLevel, callback) {
    var payments = {};

    for (var worker in balances) {
        var balance = balances[worker];
        if (balance >= minPayoutLevel[worker]) {
            var remainder = balance % config.payments.denomination;
            var payout = balance - remainder;
            // if use dynamic transfer fee, fee will be subtracted from miner's payout
            if(config.payments.useDynamicTransferFee && config.payments.minerPayFee){
                payout -= config.payments.transferFeePerPayee;
            }
            if (payout < 0) continue;
            payments[worker] = payout;
        }
    }

    if (Object.keys(payments).length === 0){
        log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
        callback(true);
        return;
    }
    console.log("PAYMENTS:");
    console.log(payments);
    
    callback(null, payments);
}

function txCmd() {
    this.redis = [];
    this.amount = 0;
    this.dest = [];
    this.fee = config.payments.transferFee;
}

// send out the payments!
function payoutPayments(payments, callback) {
    var txcmds = [];
    var tx = new txCmd();
    txcmds.push(tx);
    
    for (var worker in payments) {
        var amount = parseInt(payments[worker]);
        if(config.payments.maxTransactionAmount) {
            if ((amount + tx.amount) > config.payments.maxTransactionAmount) {
                amount = config.payments.maxTransactionAmount - tx.amount;
            }
        }
        
        tx.dest.push({amount: amount, address: worker});
        tx.redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
        if(config.payments.useDynamicTransferFee && config.payments.minerPayFee){
            tx.redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -config.payments.transferFeePerPayee]);
        }
        tx.redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
        tx.amount += amount;
        
        if (config.payments.maxTransactionAmount && tx.amount >= config.payments.maxTransactionAmount) {
            // update payment fee if use dynamic transfer fee
            if(config.payments.useDynamicTransferFee) {
                tx.fee = config.payments.transferFeePerPayee * tx.dest.length;
            }
            var tx = new txCmd();
            txcmds.push(tx);
        }
    }
    
    console.log("Transfers:");
    console.log(tx);
    
    var timeOffset = 0;
    
    async.filter(txcmds, function(tx, cback) {
        var args =  {
            destinations: tx.dest,
            fee: tx.fee,
            mixin: config.payments.mixin,
            unlock_time: 0
        };
        
        apiInterfaces.rpcWallet('transfer', args, function(error, result) {
            if (error){
                log('error', logSystem, 'Error with transfer RPC request to wallet daemon %j', [error]);
                log('error', logSystem, 'Payments failed to send to %j', tx.dest);
                cback(false);
                return;
            }
            
            var txHash = result.tx_hash.replace(/(\<|\>)/g, '');
            var now = (timeOffset++) + Date.now() / 1000 | 0;
            
            tx.redis.push(['zadd', config.coin + ':payments:all', now, [
                txHash,
                tx.amount,
                config.payments.transferFee,
                config.payments.mixin,
                Object.keys(tx.dest).length
            ].join(':')]);
            
            for (var i = 0; i < tx.dest.length; i++){
                var destination = tx.dest[i];
                tx.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                    txHash,
                    destination.amount,
                    config.payments.transferFee,
                    config.payments.mixin
                ].join(':')]);
            }
            
            log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
            redisClient.multi(tx.redis).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
                    log('error', logSystem, 'Double payments likely to be sent to %j', tx.dest);
                    cback(false);
                    return;
                }
                cback(true);
            });
        });
    }, function(succeeded){
        var failedAmount = txcmds.length - succeeded.length;
        log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);
        callback(null);
    });
}

function runPayments() {
    async.waterfall([
        getWorkers,
        getWorkerBalances,
        filterBalances,
        payoutPayments
    ], function() {
        console.log("RUN PAYMENTS AGAIN IN ...");
        setTimeout(runPayments, config.payments.interval * 1000);
    });
}

runPayments();

