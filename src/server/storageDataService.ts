
import {MongoClient} from 'mongodb'
import {Conversation} from './conversationManager'
import {IActivity} from '../types/activityTypes'

const url = "mongodb://localhost:27017/"
const storagePath =`${url}botStorage`

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

    public SaveActivities = saveActivities;
}

export const storageDataService = new StorageDataService();

/**
 * Save the new data in storage or update existing data (Warning! existing data will overrides)
 * @param {DataType} type determinates the type of data collection
 * @param {Object} data document object
 * @param {Function} callback callback function 
 */
function saveData (type:DataType, data:Object, callback:Function){
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
 * Save the new conversation with all messages or updates existing data (insert new messages and updates the last watermark) 
 * @param {Conversation} data conversation object
 * @param {Function} callback callback function
 */
export const saveActivities = (data:Conversation, callback:Function)=>{
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
                            console.log("Data was saved");
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
                        getMaxWatermark(data, (err, maxVal)=>{
                            collection.update({"_id":data["botid"], "conversations.conversationId":data["conversationId"]}, 
                                {$set:{"conversations.$.lastWatermark":maxVal}},
                                function(err, res){
                                    collection.update({"_id":data["botid"], "conversations.conversationId":data["conversationId"]},
                                    {$addToSet:{"conversations.$.activities":{$each:data["activities"]}}},
                                    function(err, res){
                                        console.log(`Messages for conversation ${data["conversationId"]} was saved`);
                                        callback(err,res);                                
                                    });
                                });                                    
                            });
                    }else{
                        collection.update({"_id":data["botid"]}, 
                            {$addToSet:{"conversations":data}},
                            function(err, res){
                                console.log("Data was saved");
                                callback(err,res);
                        })
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
export const initBotConversation = (data: Object)=>{
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
                            console.log(`Bot ${data["_id"]} document created.`);
                    });
                }else{
                    saveActivities(data["conversations"][0], ()=>{});
                    console.log(`Bot ${data["_id"]} document exists.`);
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
                    ? maxval[0]["lastWatermark"] | 0
                    : 0;
                data["activities"].forEach((item)=>{
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
            countConversations(id, (err, maxSkip)=>{
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