
import {MongoClient} from 'mongodb'
import {Conversation} from './conversationManager'
import {IActivity} from '../types/activityTypes'
import * as Settings from './settings'

let storagePath = '';
setTimeout(()=>{ 
    storagePath = Settings.getStore().getState().framework.botStoragePath;
}, 1000);

export enum DataType{
    Message,
    BotState
}

class StorageDataService{
    /**
     * Save the new data in storage or update existing data (Warning! existing data will overrides)
     * @param {DataType} type determinates the type of data collection
     * @param {Object} data document object
     * @param {Function} callback callback function 
     */
    public SaveData = saveData;

    /**
     * Save the new conversation with all messages or updates existing data (insert new messages and updates the last watermark) 
     * @param {Conversation} data conversation object
     * @param {Function} callback callback function
     */
    public SaveActivities = saveActivities;

    /**
     * Add single activity in conversation
     * @param {string} botId bot identifier
     * @param {string} conversationId conversation identifier
     * @param {IActivity} activity activity object
     * @param {Function} callback callback function
     */
    public AddActivity = addActivity;

    /**
     * Initialize bot conversation
     * @param {object} data bot conversation object
     */
    public InitBotConversation = initBotConversation;

    /**
     * Get saved data by identifier
     * @param {DataType} type determinates collection where data is stored
     * @param {string} id criteria of search: the identifier
     * @param {Function} callback callback function
     */
    public GetDataById = getDataById;

    /**
     * Get saved data by Qyery (object)
     * @param {DataType} type determinates collection where data is stored
     * @param {string} id criteria of search: the query object with mongo query options
     * @param {Function} callback callback function
     * @param {number} limit [optional] limit the number of records
     * @param {number} skip [optional] skip the number of records
     */
    public GetDataByQuery = getDataByQuery;

    /**
     * Gets the specified amount of last messsages
     * @param {number} position last position of readed messages
     * @param {number} count Count of last readed messages
     * @param {string} botId Bot identifier
     * @param {string} conversationId conversation identifier
     * @param {Function} callback callback function
     */
    public GetLastMessages = getLastMessages;

    /**
     * Gets the count of the messages in document
     * @param {string} botid bot uniquie identifier
     * @param {string} conversationId conversation uniquie identifier
     * @param {Function} callback callback function
     */
    public CountActivities = countActivities;
}

export const storageDataService = new StorageDataService();

function fakeCallback(err){
    if(err){
        throw err;
    }
};

/**
 * Save the new data in storage or update existing data (Warning! existing data will overrides)
 * @param {DataType} type determinates the type of data collection
 * @param {Object} data document object
 * @param {Function} callback callback function 
 */
function saveData (type:DataType, data:Object, callback:Function){
    callback = callback ? callback : fakeCallback;
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
                        //console.log("Data was saved");
                        callback(err,res);
                })
            });         
        });
    });
}

/**
 * Save the new conversation with all messages or updates existing data (insert new messages and updates the last watermark) 
 * @param {Conversation} data conversation object
 * @param {Function} callback callback function
 */
function saveActivities (data:Conversation, callback:Function){
    callback = callback ? callback : fakeCallback;
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
            countConversations(data["botid"],(err, convCount)=>{
                if(!convCount){
                    collection.update({"_id":data["botid"]}, 
                        {$addToSet:{"conversations":data}},
                        function(err, res){
                            //console.log("Data was saved");
                            callback(err,res);
                    })
                    return;
                }
                let query = [
                    {$match:{"_id":data["botid"]}},
                    {$unwind:"$conversations"},
                    {$project:{"conversationId":"$conversations.conversationId"}},
                    {$match:{ "conversationId":data["conversationId"]}}
                ];
                collection.aggregate(query, function(err,res){
                    if(err) {
                        callback(err,null);
                        return;
                    }
                    if(res && res.length > 0){
                        let baseData = res[0];
                        data["activities"].forEach((item=>{
                            item["watermark"] = nextWatermark(data["botid"], data["conversationId"]);
                        }));
                        getMaxWatermark(data, (err, maxVal)=>{
                            collection.update({"_id":data["botid"], "conversations.conversationId":data["conversationId"]}, 
                                {$set:{"conversations.$.lastWatermark":maxVal}},
                                function(err, res){
                                    collection.update({"_id":data["botid"], "conversations.conversationId":data["conversationId"]},
                                    {$addToSet:{"conversations.$.activities":{$each:data["activities"]}}},
                                    function(err, res){
                                        //console.log(`Messages for conversation ${data["conversationId"]} was saved`);
                                        callback(err,res);                                
                                    });
                                });                                    
                            });
                    }else{
                        collection.update({"_id":data["botid"]}, 
                            {$addToSet:{"conversations":data}},
                            function(err, res){
                                //console.log("Data was saved");
                                callback(err,res);
                        })
                    }
                });
            });         
        });
    });
}

/**
 * Add single activity in conversation
 * @param {string} botId bot identifier
 * @param {string} conversationId conversation identifier
 * @param {IActivity} activity activity object
 * @param {Function} callback callback function
 */
function addActivity(botId:string, conversationId:string, activity:IActivity, callback:Function){
    callback = callback ? callback : fakeCallback;
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
            countConversations(botId, (err, convCount)=>{
                if(!convCount){
                    callback(err, null);
                    return;
                }
                let query = [
                    {$match:{"_id":botId}},
                    {$unwind:"$conversations"},
                    {$project:{"conversationId":"$conversations.conversationId"}},
                    {$match:{ "conversationId":conversationId}}
                ];
                collection.aggregate(query, function(err,res){
                    if(err) {
                        callback(err,null);
                        return;
                    }
                    if(res && res.length > 0){                      
                        activity["watermark"] =  nextWatermark(botId,conversationId);
                        let baseData = res[0];
                        baseData.activities = [activity];
                        getMaxWatermark(baseData, (err, maxVal)=>{
                            collection.update({"_id":botId, "conversations.conversationId":conversationId}, 
                                {$set:{"conversations.$.lastWatermark":maxVal}},
                                function(err, res){
                                    collection.update({"_id":botId, "conversations.conversationId":conversationId},
                                    {$addToSet:{"conversations.$.activities":activity}},
                                    function(err, res){
                                        //console.log(`New message for conversation ${conversationId} was saved`);
                                        callback(err,res);                                
                                    });
                                });                                    
                            });
                    }
                });
            });         
        });
    });
}

/**
 * Initialize bot conversation
 * @param {object} data bot conversation object
 */
function initBotConversation (data: Object){
    let cacheWatermark = (err)=>{
        if(err){
            throw err;
        }
        getMaxWatermark({"botid":data["_id"], 
            "conversationId":data["conversations"][0]["conversationId"]}, 
            (err, lastWatermark)=>{
                let id = `${data["_id"]}/${data["conversations"][0]["conversationId"]}`;
                watermarks[id] = lastWatermark;
            });
    };
    MongoClient.connect(storagePath, function(err, db){
        if(err) {
            throw err;
        }
        db.createCollection(`${DataType[DataType.Message]}s`, function(err, collection){
            if(err) {
                throw err;
            }
            collection.findOne({"_id":data["_id"]}, function(err,res){
                if(err) {
                    throw err;
                }
                if(!res){
                    collection.save(data, 
                        function(err, res){
                            //console.log(`Bot ${data["_id"]} document created.`);
                            cacheWatermark(null);
                    });
                }else{
                    saveActivities(data["conversations"][0], cacheWatermark);
                    //console.log(`Bot ${data["_id"]} document exists.`);
                }
            });
        });
    });
};

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
        db.collection(`${DataType[DataType.Message]}s`, (err, collection)=>{
            if(err){
                callback(err, null);
                return;
            }
            var query = [
                {$match:{"_id":data["botid"]}},
                {$unwind:"$conversations"},
                {$project:{"lastWatermark":"$conversations.lastWatermark", "conversationId":"$conversations.conversationId"}},
                {$match:{"conversationId":data["conversationId"]}}
            ];
            collection.aggregate(query, (err, maxval)=>{
                maxVal = maxval && maxval.length && maxval.length > 0 
                    ? maxval[0]["lastWatermark"]
                    : 0;
                let cachedWatermark = watermarks[`${data["botid"]}/${data["conversationId"]}`];
                if(cachedWatermark){
                    maxval = cachedWatermark > maxVal
                        ? cachedWatermark
                        : maxval;
                }
                
                if(data["activities"]){
                    data["activities"].forEach((item)=>{
                        if(item["watermark"]>maxVal){
                            maxVal = item["watermark"];
                        }
                    });
                }
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
function getDataById (type:DataType, id:string, callback:Function){
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
function getDataByQuery (type:DataType, query:Object, callback:Function, limit?:number, skip?:number){
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
 * @param {number} position last position of readed messages
 * @param {number} count Count of last readed messages
 * @param {string} botId Bot identifier
 * @param {string} conversationId conversation identifier
 * @param {Function} callback callback function
 */
function getLastMessages (position:number, count:number, botId:string, conversationId:string, callback:Function){    
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
            countActivities(botId, conversationId, (err, maxSkip)=>{
                if(err){
                    callback(err, null);
                    return;
                }                
                let skip = position;
                if(maxSkip<skip){
                    count = skip - maxSkip;
                    if(count <0){
                        count = 0;
                    }
                    skip = maxSkip;
                }
                let query = [
                    {$match:{"_id":botId}},
                    {$unwind:"$conversations"},
                    {$project:{
                        "_id":false,
                        "conversationId":"$conversations.conversationId",
                        "activities":"$conversations.activities"
                    }},
                    {$sort:{"activities.watermark":1}},
                    {$match:{ "conversationId":conversationId}},
                    {$project:{
                        "activities":{$slice:["$activities", -skip, count]}
                    }}
                ];
                collection.aggregate(query, function(err,res){
                    let msgs = res?res[0]["activities"]:undefined;
                    callback(err, msgs);
                });
            });
        });
    });
};

/**
 * Gets the count of the conversations in document
 * @param {string} id document uniquie identifier
 * @param {Function} callback callback function
 */
function countConversations(id:string, callback:Function){
    MongoClient.connect(storagePath, (err, db)=>{
        if(err){
            callback(err, null);
            return;
        }
        let collection = db.collection(`${DataType[DataType.Message]}s`);
        var query = [
            {$match:{"_id":id}},
            {$project:{"total":{$size:"$conversations"}}}
        ];
        collection.aggregate(query, (err, res)=>{  
            let total = res && res.length>0 ?res[0]["total"]:undefined;
            callback(err, total);
        });
    });
}

/**
 * Gets the count of the messages in document
 * @param {string} botid bot uniquie identifier
 * @param {string} conversationId conversation uniquie identifier
 * @param {Function} callback callback function
 */
function countActivities(botid:string, conversationId:string, callback:Function){
    MongoClient.connect(storagePath, (err, db)=>{
        if(err){
            callback(err, null);
            return;
        }
        let collection = db.collection(`${DataType[DataType.Message]}s`);
        var query = [
            {$match:{"_id":botid}},            
            {$unwind:"$conversations"},
            {$match:{"conversations.conversationId":conversationId}},
            {$project:{"total":{$size:"$conversations.activities"}}}
        ];
        collection.aggregate(query, (err, res)=>{  
            let total = res && res.length>0 ?res[0]["total"]:undefined;
            callback(err, total);
        });
    });
}

/**
 * Watermarks cache
 */
const watermarks = {};

/**
 * Fake generator for watermark sequence
 * @param {string} botid bot Identifier
 * @param {string} conversationId conversation Identifier
 */
function nextWatermark(botid:string, conversationId:string):number{
    var id = `${botid}/${conversationId}`;
    var num = watermarks[id]
    if(num == undefined || num == null){
        watermarks[id] = 0;
        return watermarks[id];
    }else{
        watermarks[id] += 1;
        return watermarks[id];
    }
}