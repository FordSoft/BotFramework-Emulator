
import {MongoClient} from 'mongodb'
const url = "mongodb://localhost:27017/"


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
    let storagePath = `${url}botStorage`;
    
    MongoClient.connect(storagePath, function(err, db){
        if(err) {
            callback(err,null);
        }
        db.createCollection(`${DataType[type]}s`, function(err, collection){
            if(err) {
                callback(err,null);
            }
            collection.findOne({"_id":data["_id"]}, function(err,res){
                if(err) {
                    callback(err,null);
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
    let storagePath = `${url}botStorage`;
    
    MongoClient.connect(storagePath, function(err, db){
        if(err) {
            callback(err,null);
        }
        db.createCollection(`${DataType[DataType.Message]}s`, function(err, collection){
            if(err) {
                callback(err,null);
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

    let storagePath = `${url}botStorage`;
    MongoClient.connect(storagePath, (err, db)=>{
        db.collection(`Messages`, (err, collection)=>{
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
 * @param {number} limit limit the number of records
 * @param {number} skip skip the number of records
 */
export const getDataByQuery = (type:DataType, query:Object, callback:Function, limit?:number, skip?:number)=>{
    let storagePath = `${url}botStorage`;

    MongoClient.connect(storagePath, function(err, db){
        if(err){
            callback(err, null);
        }
        db.collection(`${DataType[type]}s`, function(err, collection){
            if(err){
                callback(err, null);
            }
            var cursor = collection.find(query);
            if(skip){
                cursor.skip(skip);
            }
            if(limit){
                cursor.limit(limit);
            }
            cursor.toArray(function(err, res){
                console.log(res);
                callback(err, res)
            });
        });
    });
};

export const getLastMessages =()=>{};