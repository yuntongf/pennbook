var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var async = require('async');
var sha256 = require('js-sha256');

// THIS LOADER IS ONLY FOR USERS AND FRIENDS
var list_users = [ ["sample_user", "Mickey", "Mouse", sha256("mickeymouse"), "mickey@outlook.com", "University of Mousylvania", "2002-03-23","POLITICS, WELLNESS, ENTERTAINMENT"],
["sample_user2", "Amy", "Gutmann", sha256("amyg"), "gamy@upenn.edu", "Penn", "2002-01-03","WELLNESS, ENTERTAINMENT"],
["sample_user3", "Chris", "Robin", sha256("crobin"), "crobin@seas.upenn.edu", "SEAS", "2002-11-12","POLITICS, WELLNESS, TRAVEL"],
["sample_user4", "Chatterjee", "Partha", sha256("parthac"), "chatt@seas.upenn.edu", "University of Mousylvania", "1974-10-12","POLITICS, ENTERTAINMENT"] ];

var list_friends = [["sample_user/sample_user2","sample_user","sample_user2","Mickey","Mouse","Amy","Gutmann","Penn"],
["sample_user2/sample_user","sample_user2","sample_user","Amy","Gutmann","Mickey","Mouse","University of Mousylvania"],
["sample_user2/sample_user3","sample_user2","sample_user3","Amy","Gutmann","Chris","Robin","SEAS"],
["sample_user3/sample_user2","sample_user3","sample_user2","Chris","Robin","Amy","Gutmann","Penn"],
["sample_user2/sample_user4","sample_user2","sample_user4","Amy","Gutmann","Chatterjee","Partha","University of Mousylvania"],
["sample_user4/sample_user2","sample_user4","sample_user2","Chatterjee","Partha","Amy","Gutmann","Penn"] ];

var initTable = function(tableName, keyname, callback) {
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
                			AttributeName: keyname,
                			AttributeType: 'S'
              			}
            			],
          			KeySchema: 
            			[ 
              			{
                			AttributeName: keyname,
                			KeyType: 'HASH'
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
                	AttributeName: keyname,
                	AttributeType: 'S'
              	}
            	],
          	KeySchema: 
            	[ 
              	{
                	AttributeName: keyname,
                	KeyType: 'HASH'
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

var putIntoUserTable = function(attr, callback) {
  var params = {
      Item: {
        "username": {
          S: attr[0]
        },
        "firstname": { 
          S: attr[1]
        },
        "lastname": {
		  S: attr[2]
		},
		"password": {
		  S: attr[3]
		},
		"email": {
		  S: attr[4]
		},
		"affiliation": {
		  S: attr[5]
		},
		"birthday": {
		  S: attr[6]
		},
		"interests": {
		  S: attr[7]
		}
      },
      TableName: "users",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}

var putIntoFriendsTable = function(attr, callback) {
  var params = {
      Item: {
		// the unique id for this friend relation
        "id": {
          S: attr[0]
        },
        // the username of user A
        "idA": { 
          S: attr[1]
        },
        // the username of user B
        "idB": {
		  S: attr[2]
		},
		// the first name of user A
		"firstNameA": {
		  S: attr[3]
		},
		// the last name of user A
		"lastNameA": {
		  S: attr[4]
		},
		// the first name of user B
		"firstNameB": {
		  S: attr[5]
		},
		// the last name of user B
		"lastNameB": {
		  S: attr[6]
		},
		// the affiliation of user B is needed for friend visualization
		"affiliationB": {
		  S: attr[7]
		}
      },
      TableName: "friends",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}


initTable("users", "username", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(list_users, function (usr, callback) {
      console.log("Uploading user: " + usr[0]);
      putIntoUserTable(usr, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+usr[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});

initTable("friends", "id", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(list_friends, function (friend, callback) {
      console.log("Uploading friend pair: " + friend[0]);
      putIntoFriendsTable(friend, function(err, data) {
        if (err)
          console.log("Oops, error when adding "+friend[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});
