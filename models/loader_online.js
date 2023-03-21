var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var async = require('async');

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

var putIntoUserTable = function(callback) {
  var params = {
      Item: {
        "username": {
          S: 'sample_user'
        },
		"online": {
		  S: 'Yes'
		}
      },
      TableName: "online_users",
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
    if (err)
      callback(err)
    else
      callback(null, 'Success')
  });
}


initTable("online_users", "online", "online", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    putIntoUserTable(function(err, data) {
        if (err)
          console.log("Oops, error when adding "+usr[0]+": " + err);
      });
  }
});