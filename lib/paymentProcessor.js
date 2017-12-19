var fs = require('fs');

var async = require('async');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);


var logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);
var emailSystem = require('./email.js');


log('info', logSystem, 'Started');

function Worker(address) {
    this.address = address;
    this.minPayoutLevel = 0;
    this.key = config.coin + ":workers:" + address;
    this.balance = 0;
    this.resolve = false;
    this.reject = false;
    
    self = this;
    
    this.promise = new Promise(function(resolve, reject) {
        self.resolve = resolve;
        self.reject = reject;
    });
};

// Get workers
function getWorkers(callback) {
    var promises = [];
    
    function getWorkersScan(cursor) {
        // TODO: add the count here. That way we *are* doing payments in lots
        // of X workers.
        redisClient.scan(cursor, config.coin + ':workers:*', "COUNT", config.payments.maxAddresses, function(error, reply) {
            var cursor = parseInt(reply[0]);
            var workers = reply[1];
            
            if (workers.length > 0) {
                callback(null, workers.map(function(worker) {
                    var parts = worker.split(':');
                    var w = new Worker(parts[parts.length -1]);
                    
                    promises.push(w.promise);
                    return w;
                }));
            }
            
            if (cursor)
            {
                getWorkersScan(cursor);
            }
            else
            {
                // Force flushing of grouped payments
                callback(true, promises);
            }
        });
    }
    getWorkersScan(0);
}

// Get balances for workers
function getWorkerBalances(workers, callback) {
    // get all the balances
    var redisCommands = workers.map(function(w) {
        return ['hmget', w.key, 'balance', 'minPayoutLevel'];
    });
    
    redisClient.multi(redisCommands).exec(function(error, replies){
        if (error){
            log('error', logSystem, 'Error with getting balances from redis %j', [error]);
            callback(true);
            return;
        }
        for (var i = 0; i < replies.length; i++){
            var worker = workers[i];
            data = replies[i];
            worker.balance = parseInt(data[0]) || 0;
            worker.minPayoutLevel = parseInt(data[1]) || config.payments.minPayment;
            log('info', logSystem, 'Using payout level %d for worker %s (default: %d)', 
                [worker.minPayoutLevel, worker.address, config.payments.minPayment]);
        }
        callback(null, workers);
    });
}

//Filter workers under balance threshold for payment
function filterBalances(workers, callback) {
    workers = workers.map(function(w) {
        var remainder = w.balance % config.payments.denomination;
        w.payout = (w.balance - remainder);
        return w;
    });
    workers = workers.filter(function(w) {
        var rc = w.payout - minerTransferFee() > w.minPayoutLevel &&
            w.payout - minerTransferFee() > 0;
        if (rc == false) {
            w.resolve();
        }
        return rc;
    });
    
    if (workers.length === 0){
        log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
        return;
    }
    
    callback(null, workers);
}

function txCmd() {
    this.redis = [];
    this.amount = 0;
    this.dest = [];
    this._fee = config.payments.transferFee;
    
    this.fee = function() {
        // update payment fee if use dynamic transfer fee
        if(config.payments.useDynamicTransferFee) {
            return config.payments.transferFeePerPayee * tx.dest.length;
        } else {
            return this._fee;
        }
    }
}

function minerTransferFee() {
    if(config.payments.useDynamicTransferFee && config.payments.minerPayFee){
        return config.payments.transferFeePerPayee;
    }
    return 0;
}

// send out the payments!
function payoutPayments(workers, callback) {
    var txcmds = [];
    var tx = new txCmd();
    txcmds.push(tx);
    
    for (var id in workers) {
        var worker = workers[id];
        if (config.payments.maxTransactionAmount && tx.amount >= config.payments.maxTransactionAmount) {
            var tx = new txCmd();
            txcmds.push(tx);
        }
        
        var amount = parseInt(worker.payout);
        if(config.payments.maxTransactionAmount) {
            if ((amount + tx.amount) > config.payments.maxTransactionAmount) {
                amount = config.payments.maxTransactionAmount - tx.amount;
            }
        }
        
        tx.dest.push({amount: amount - minerTransferFee(), address: worker.address});
        tx.redis.push(
            ['hincrby', config.coin + ':workers:' + worker.address, 'balance', -amount],
            ['hincrby', config.coin + ':workers:' + worker.address, 'paid', amount - minerTransferFee()]
        );
        tx.amount += amount;
    }
    
    var timeOffset = 0;
    
    async.filter(txcmds, function(tx, cback) {
        var args =  {
            destinations: tx.dest,
            fee: tx.fee(),
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
        
    }, function(succeeded) {
        var failedAmount = txcmds.length - succeeded.length;
        log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed',
            [succeeded.length, failedAmount]);
        workers.forEach(function(worker) {
           worker.resolve();
        });
        //callback(null);
    });
}

function runPayments() {
    async.waterfall([
        getWorkers,
        getWorkerBalances,
        filterBalances,
        payoutPayments
    ], function(err, promises) {
        if (err) {
            Promise.all(promises).then(function() {
                log('info', logSystem, "Running payments again");
                setTimeout(runPayments, config.payments.interval * 1000);
            });
        }
    });
}

runPayments();
