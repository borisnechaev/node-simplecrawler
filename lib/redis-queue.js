var redis = require("redis"),
    client = redis.createClient();

// client.select(8, function() { /* ... */ });

client.on("error", function (err) {
    console.log("Error " + err);
});

var FetchQueue = function(){

};

module.exports = FetchQueue;

FetchQueue.prototype.init = function(callback) {
	callback = callback && callback instanceof Function ? callback : function(){};
	var self = this;

	client.del("hashes");
	client.del("fifo");
};

// Add a new item to the queue
FetchQueue.prototype.add = function(protocol,domain,port,path,retries,callback) {
	callback = callback && callback instanceof Function ? callback : function(){};
	var self = this;

	// Ensure all variables conform to reasonable defaults
	protocol = protocol === "https" ? "https" : "http";

	if (isNaN(port) || !port) {
		return callback(new Error("Port must be numeric!"));
	}

	var url = protocol + "://" + domain + (port !== 80 ? ":" + port : "") + path;

	var queueItem = {
		"url": url,
		"protocol": protocol,
		"host": domain,
		"port": port,
		"path": path,
		"retries": retries,
		"fetched": false,
		"status": "queued",
		"stateData": {}
	};

	client.hexists('hashes', url, function(error, res) {
		if (error) { 
			callback(new Error("Error fetching from Redis: ", error));
		} else {
			if (res == 0) {
				client.hset('hashes', url, JSON.stringify(queueItem));
				client.rpush('fifo', url);
				callback(null,queueItem);
			} else {
				var error = new Error("Resource already exists in queue!");
				error.code = "DUP";

				callback(error);
			}
		}
	});
};

// Return to the queue after a retry
FetchQueue.prototype.returnToQueue = function(queueItem,callback) {
	callback = callback && callback instanceof Function ? callback : function(){};
	var self = this;

	client.hset('hashes', queueItem.url, JSON.stringify(queueItem));
	client.lpush('fifo', queueItem.url);
};

// Get first unfetched item in the queue (and return its index)
FetchQueue.prototype.oldestUnfetchedItem = function(callback) {
	callback = callback && callback instanceof Function ? callback : function(){};
	var item, self = this;

	client.lpop('fifo', function(error, url){
	    if (error) { 
	    	callback(new Error("Error fetching from Redis: ", error));
	    } else {
	    	client.hget("hashes", url, function(error, obj) {
	    		if (error) { 
	    			callback(new Error("Error fetching from Redis: ", error));
	    		} else {
		    		item = JSON.parse(obj);
		    		callback(null, item);
	    			return item;
	    		}
			});	    	
	    }	    
	});	
};

// Return the number of items in the queue
FetchQueue.prototype.getLength = function(callback) {
	callback = callback && callback instanceof Function ? callback : function(){};
	var self = this;

	var length = 0;
	client.llen('fifo', function(error, length){
	    if (error) { 
	    	callback(new Error("Error fetching from Redis: ", error));
	    } else {
			if (!length) {
				length = 0;
			}
			callback(null,length);
			return length;
	    }
	});	
};