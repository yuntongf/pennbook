var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var async = require('async');
var sha256 = require('js-sha256');
var uuid = require('uuid');

// THIS LOADER IS ONLY FOR POSTS AND COMMETS
var list_posts = [[uuid.v1(), Date.now().toString(),"sample_user", "sample_user2", "I love you mickey"],
[uuid.v1(), Date.now().toString(), "sample_user2","sample_user2", "I am AMY G"]];

var list_comments = [[uuid.v1(), Date.now().toString(), list_posts[0][0], "sample_user", "great post!"],
[uuid.v1(), Date.now().toString(), list_posts[1][0], "sample_user1", "bad post!"]];

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
                			AttributeType: 'N'
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
                	AttributeType: 'N'
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

var putIntoPostsTable = function(attr, callback) {
  var params = {
      Item: {
        "id": {
          S: attr[0]
        },
        "datatime": {
          N: attr[1]
        },
        "walluser_id": {
		  S: attr[2]
		},
        "poster_id": { 
          S: attr[3]
        },
        "content": {
		  S: attr[4]
		}
      },
      TableName: "posts",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

var putIntoCommentsTable = function(attr, callback) {
  var params = {
      Item: {
        "id": {
          S: attr[0]
        },
        "datatime": {
          N: attr[1]
        },
        "post_id": {
		  S: attr[2]
		},
		"commenter_id": {
		  S: attr[3]
		},
        "content": {
		  S: attr[4]
		}
      },
      TableName: "comments",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

initTable("posts", "id", "datatime", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(list_posts, function (post, callback) {
      console.log("Uploading post: " + post[0]);
      putIntoPostsTable(post, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+post[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});

initTable("comments", "id", "datatime", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(list_comments, function (comment, callback) {
      console.log("Uploading comment: " + comment[0]);
      putIntoCommentsTable(comment, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+comment[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});