var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var async = require('async');
var sha256 = require('js-sha256');
var uuid = require('uuid');

var chatID1 = uuid.v1();
var chatID2 = uuid.v1();
var chatID3 = uuid.v1();
var chatID4 = uuid.v1();

var sample_chat_messages = [
	[chatID1, uuid.v1(), "sample_user", "Good morning!", Date.now().toString()],
	[chatID1, uuid.v1(), "sample_user2", "Good morning, mickey!", Date.now().toString()],
	[chatID2, uuid.v1(), "sample_user", "Hello!", Date.now().toString()],
	[chatID2, uuid.v1(), "sample_user2", "Hello, friends!", Date.now().toString()],
	[chatID2, uuid.v1(), "sample_user3", "I'm also in the groupchat.'", Date.now().toString()]
];

var sample_chatrooms = [
	[chatID1, "sample_user"],
	[chatID1, "sample_user2"],
	[chatID2, "sample_user"],
	[chatID2, "sample_user2"],
	[chatID2, "sample_user3"],
];

var sample_userchats = [
	["sample_user", chatID1],
	["sample_user", chatID2],
	["sample_user2", chatID1],
	["sample_user2", chatID2],
	["sample_user3", chatID2],
];

var sample_invites = [
	["sample_user4", chatID2, "sample_user"],
	["sample_user3", chatID1, "sample_user2"],
	["sample_user", chatID3, "sample_user2"],
	["sample_user", chatID4, "sample_user3"],
	
]

var initTable = function(tableName, pkeyname, skeyname, callback) {
  db.listTables(function(err, data) {
    if (err)  {
      console.log(err, err.stack);
      callback('Error when listing tables: '+err, null);
    } else {
      console.log("Connected to AWS DynamoDB");
          
      var tables = data.TableNames.toString().split(",");
      console.log("Tables in DynamoDB: " + tables);
      if (tables.includes(tableName)) {
		var params = {
			TableName: tableName
		};
		db.deleteTable(params, function(err, data){
			console.log(tableName+" deleted");
			if (err) {
				console.log(err);
				callback('Error while deleting existing table '+tableName+': '+err, null);
			} else  {
				console.log("Creating new table '"+tableName+"'");

      			var params = {
          			AttributeDefinitions: 
            			[ 
              			{
                			AttributeName: pkeyname,
                			AttributeType: 'S'
              			},
              			{
                			AttributeName: skeyname,
                			AttributeType: 'S'
              			}
            			],
          			KeySchema: 
            			[ 
              			{
                			AttributeName: pkeyname,
                			KeyType: 'HASH'
              			},
              			{
                			AttributeName: skeyname,
                			KeyType: 'RANGE'
              			}
            			],
          			ProvisionedThroughput: { 
            			ReadCapacityUnits: 20,       // DANGER: Don't increase this too much; stay within the free tier!
            			WriteCapacityUnits: 20       // DANGER: Don't increase this too much; stay within the free tier!
          			},
          			TableName: tableName /* required */
      			};

      			db.createTable(params, function(err, data) {
        			if (err) {
          				console.log(err)
          				callback('Error while creating table '+tableName+': '+err, null);
        			}
        			else {
          				console.log("Table is being created; waiting for 20 seconds...");
          				setTimeout(function() {
            				console.log("Success");
            				callback(null, 'Success');
          				}, 20000);
        			}
      			});
			}
		})
	  } else {
		console.log("Creating new table '"+tableName+"'");

      	var params = {
          	AttributeDefinitions: 
            	[ 
              	{
                	AttributeName: pkeyname,
                	AttributeType: 'S'
              	},
              	{
                	AttributeName: skeyname,
                	AttributeType: 'S'
              	}
            	],
          	KeySchema: 
            	[ 
              	{
                	AttributeName: pkeyname,
                	KeyType: 'HASH'
              	},
              	{
                	AttributeName: skeyname,
                	KeyType: 'RANGE'
              	}
            	],
          	ProvisionedThroughput: { 
            	ReadCapacityUnits: 20,       // DANGER: Don't increase this too much; stay within the free tier!
            	WriteCapacityUnits: 20       // DANGER: Don't increase this too much; stay within the free tier!
          	},
          	TableName: tableName /* required */
      	};

      	db.createTable(params, function(err, data) {
        	if (err) {
          		console.log(err)
          		callback('Error while creating table '+tableName+': '+err, null);
        	}
        	else {
          		console.log("Table is being created; waiting for 20 seconds...");
          		setTimeout(function() {
            		console.log("Success");
            		callback(null, 'Success');
          		}, 20000);
        	}
      	});
	}
      
    }
  });
}

var putIntoChatsTable = function(attr, callback) {
  var params = {
      Item: {
		"msg_id": {
          S: attr[1]
        },
        "chat_id": {
          S: attr[0]
        },
        ["sender"]: {
		  S: attr[2]
		},
        ["content"]: {
		  S: attr[3]
		},
		["datatime"]: {
          N: attr[4]
        },
      },
      TableName: "chats",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

var putIntoChatroomsTable = function(attr, callback) {
  var params = {
      Item: {
        "chat_id": {
          S: attr[0]
        },
        "member": {
          S: attr[1]
        }
      },
      TableName: "chatrooms",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

var putIntoUserChatsTable = function(attr, callback) {
  var params = {
      Item: {
        "user_id": {
          S: attr[0]
        },
        "chat_id": {
          S: attr[1]
        }
      },
      TableName: "user_chats",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

var putIntoChatInvitesTable = function(attr, callback) {
  var params = {
      Item: {
        "user_id": {
          S: attr[0]
        },
        "invite_chat": {
          S: attr[1]
        },
        ["inviter"]: {
			S: attr[2]
		}
      },
      TableName: "chat_invites",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

initTable("chats", "chat_id", "msg_id", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(sample_chat_messages, function (msg, callback) {
      console.log("Uploading message: " + msg[0]);
      putIntoChatsTable(msg, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+msg[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});

initTable("chatrooms", "chat_id", "member", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(sample_chatrooms, function (room, callback) {
      console.log("Uploading chatroom: " + room[0]);
      putIntoChatroomsTable(room, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+room[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});

initTable("user_chats", "user_id", "chat_id", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(sample_userchats, function (chat, callback) {
      console.log("Uploading userchat: " + chat[0]);
      putIntoUserChatsTable(chat, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+chat[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});

initTable("chat_invites", "user_id", "invite_chat", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(sample_invites, function (invite, callback) {
      console.log("Uploading invite: " + invite[0]);
      putIntoChatInvitesTable(invite, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+invite[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});