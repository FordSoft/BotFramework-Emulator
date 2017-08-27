
import {MongoClient} from 'mongodb'
const url = "mongodb://localhost:27017/"
const storagePath =`${url}botStorage`

export enum DataType{
    Message,
    BotState
}

/**
 * Save the new data in storage or update existing data (Warning! existing data will overrides)
 * @param {DataType} type determinates the type of data collection
 * @param {Object} data conversation document object
 * @param {Function} callback callback function 
 */
export const saveData = (type:DataType, data:Object, callback:Function)=>{
    MongoClient.connect(storagePath, function(err, db){
        if(err) {
            callback(err,null);
            return;
        }
        db.createCollection(`${DataType[type]}s`, function(err, collection){
            if(err) {
                callback(err,null);
                return;
            }
            collection.findOne({"_id":data["_id"]}, function(err,res){
                if(err) {
                    callback(err,null);
                    return;
                }
                collection.save(data, 
                    function(err, res){
                        console.log("Data was saved");
                        callback(err,res);
                })
            });         
        });
    });
}

/**
 * Save the new message data or updates existing data (insert new messages and updates the last watermark) 
 * @param {Object} data conversation document object
 * @param {Function} callback callback function
 */
export const saveMessagesData = (data:Object, callback:Function)=>{
    MongoClient.connect(storagePath, function(err, db){
        if(err) {
            callback(err,null);
            return;
        }
        db.createCollection(`${DataType[DataType.Message]}s`, function(err, collection){
            if(err) {
                callback(err,null);
                return;
            }
            if(!data["_id"]){
                collection.save(data, 
                    function(err, res){
                        console.log("Data was saved");
                        callback(err,res);
                })
                return;
            }
            collection.findOne({"_id":data["_id"]}, function(err,res){
                if(err) {
                    callback(err,null);
                    return;
                }
                if(res){
                    getMaxWatermark(data, (err, maxVal)=>{
                        collection.update({"_id":data["_id"]}, 
                            {$set:{"lastWatermark":maxVal}}, 
                            function(err, res){
                                collection.update({"_id":data["_id"]}, 
                                    {$addToSet:{"messages":{$each:data["messages"]}}},
                                    function(err, res){
                                        console.log("Messages was saved");
                                        callback(err,res);                                
                                    });
                                });
                            });
                }else{
                    collection.save(data, 
                        function(err, res){
                            console.log("Data was saved");
                            callback(err,res);
                    })
                }
            });         
        });
    });
}

/**
 * Get the maximum value of watermark
 * @param {Object} data conversation document object
 * @param {Function} callback callback function
 */
function getMaxWatermark(data:Object, callback:Function){
    let maxVal = 0;
    
    MongoClient.connect(storagePath, (err, db)=>{
        if(err){
            callback(err, null);
            return;
        }
        db.collection(`Messages`, (err, collection)=>{
            if(err){
                callback(err, null);
                return;
            }
            var query = [
                {$match:{"_id":data["_id"]}},
                {$unwind:"$messages"},
                {$sort:{"messages.watermark":-1}},
                {$limit:1},
                {$project:{"messages.watermark":1}}
            ];
            collection.aggregate(query, (err, maxval)=>{
                maxVal = maxval && maxval.length && maxval.length > 0 
                    ? maxval[0]["messages"]["watermark"] 
                    : 0;
                data["messages"].forEach((item)=>{
                    if(item["watermark"]>maxVal){
                        maxVal = item["watermark"];
                    }
                });
                callback(err, maxVal);
            });
        });
    });
}

/**
 * Get saved data by identifier
 * @param {DataType} type determinates collection where data is stored
 * @param {string} id criteria of search: the identifier
 * @param {Function} callback callback function
 */
export const getDataById = (type:DataType, id:string, callback:Function)=>{
    getDataByQuery(type,{"_id":id}, (err, res)=>{
        if(err){
            callback(err, null);
            return;
        }
        if(res.length>0){
            callback(err, res[0]);
        }
        else{
            callback(null, null)
        }
    })
};

/**
 * Get saved data by Qyery (object)
 * @param {DataType} type determinates collection where data is stored
 * @param {string} id criteria of search: the query object with mongo query options
 * @param {Function} callback callback function
 * @param {number} limit [optional] limit the number of records
 * @param {number} skip [optional] skip the number of records
 */
export const getDataByQuery = (type:DataType, query:Object, callback:Function, limit?:number, skip?:number)=>{
    MongoClient.connect(storagePath, function(err, db){
        if(err){
            callback(err, null);
            return;
        }
        db.collection(`${DataType[type]}s`, function(err, collection){
            if(err){
                callback(err, null);
                return;
            }
            var cursor = collection.find(query);
            if(skip){
                cursor.skip(skip);
            }
            if(limit){
                cursor.limit(limit);
            }
            cursor.toArray(function(err, res){
                callback(err, res)
            });
        });
    });
};

/**
 * Gets the specified amount of last messsages
 * @param {number} count Count of last readed messages
 * @param {number} botId Bot identifier
 * @param {string} conversationId conversation identifier
 * @param {Function} callback callback function
 * @param {number} position [optional] last position of readed messages
 */
export const getLastMessages =(count:number, botId:number, conversationId:string, callback:Function, position?:number)=>{
    let id = `${botId}/${conversationId}`;
    MongoClient.connect(storagePath, function(err, db){
        if(err){
            callback(err, null);
            return;
        }
        db.collection(`${DataType[DataType.Message]}s`, function(err, collection){
            if(err){
                callback(err, null);
                return;
            }
            countMessages(id, (err, maxSkip)=>{
                if(err){
                    callback(err, null);
                    return;
                }                
                let skip = position + count;
                if(maxSkip<skip){
                    count = skip - maxSkip;
                    if(count <0){
                        count = 0;
                    }
                    skip = maxSkip;
                }
                let cursor = !position
                    ? collection.find({"_id": id},{ "messages":{$slice:-count}})
                    : collection.find({"_id": id},{ "messages":{$slice:[-skip, count]}}); 
                cursor.toArray(function(err, res){
                    let msgs = res?res[0]["messages"]:null
                    callback(err, msgs);
                });
            });
        });
    });
};

/**
 * Gets the count of the messages in document
 * @param {string} id message document uniquie identifier
 * @param {Function} callback callback function
 */
function countMessages(id:string, callback:Function){
    MongoClient.connect(storagePath, (err, db)=>{
        if(err){
            callback(err, null);
            return;
        }
        db.collection(`Messages`, (err, collection)=>{
            if(err){
                callback(err, null);
                return;
            }
            var query = [
                {$match:{"_id":id}},
                {$project:{"totalMsg":{$size:"$messages"}}}
            ];
            collection.aggregate(query, (err, res)=>{  
                let total = res && res.length>0 ?res[0]["totalMsg"]:0;
                callback(err, total);
            });
        });
    });
}