var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var async = require('async');
var sha256 = require('js-sha256');
var uuid = require('uuid');
var myDB = require('./models/database.js');
const fs = require('fs');
const uuidv4 = require("uuid/v4");

const ReadLines = require('n-readlines');
const readLines = new ReadLines('News_Category_Dataset_v2.json');

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
              			},
              			{
							AttributeName: "A",
							AttributeType: 'S',
						},
              			{
							AttributeName: "B",
							AttributeType: 'S',
						},
              			{
							AttributeName: "typeA",
							AttributeType: 'N',
						},
              			{
							AttributeName: "typeB",
							AttributeType: 'N',
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
            			WriteCapacityUnits: 25       // DANGER: Don't increase this too much; stay within the free tier!
          			},
          			TableName: tableName /* required */
      			};

      			db.createTable(params, function(err, data) {
        			if (err) {
          				console.log(err)
          				callback('Error while creating table '+tableName+': '+err, null);
        			}
        			else {
          				setTimeout(function() {
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
            	WriteCapacityUnits: 25       // DANGER: Don't increase this too much; stay within the free tier!
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

/*

{
	PutRequest: {
		Item: {
		    "id": {
		      S: uniqueID
		    },
		    "A": { 
		      S: attr[0]
		    },
		    "B": {
			  S: attr[1]
			},
			"typeA": {
				N: attr[2].toString()
			},
			"typeB": {
				N: attr[3].toString()
			}
		}
	}
},
{
	PutRequest: {
		Item: {
		    "id": {
		      S: uniqueID
		    },
		    "B": { 
		      S: attr[0]
		    },
		    "A": {
			  S: attr[1]
			},
			"typeB": {
				N: attr[2].toString()
			},
			"typeA": {
				N: attr[3].toString()
			}
		}
	}
}
*/

var batchNo = 0;

var addEdge = async function(attr, callback) {
	return new Promise((resolve, reject) => {
		var requestList = [];
	
		while (attr.length != 0) {
			var a = attr.shift();
			var b = attr.shift();
			var typeA = attr.shift().toString();
			var typeB = attr.shift().toString();
			var id = a + "|%|" + b;
			var item = {
				PutRequest: {
					Item: {
						"id": {
					      S: id
					    },
					    "A": { 
					      S: a
					    },
					    "B": {
						  S: b
						},
						"typeA": {
							N: typeA
						},
						"typeB": {
							N: typeB
						}
					}
				}
			}
			requestList.push(item);
			var id = b + "|%|" + a;
			var item = {
				PutRequest: {
					Item: {
						"id": {
					      S: id
					    },
					    "B": { 
					      S: a
					    },
					    "A": {
						  S: b
						},
						"typeB": {
							N: typeA
						},
						"typeA": {
							N: typeB
						}
					}
				}
			}
			requestList.push(item);
		}
		
		var params = {
			"RequestItems": {
				"news-spark": requestList
			}
		}
		
		db.batchWriteItem(params, function(err, data) {
			if (err) {
				console.log("Error:");
				console.log(err);
				resolve();
			} else {
				console.log("Writing batch: " + batchNo.toString());
				batchNo++;
				resolve(data);
			}
		});
	})
}

let line = "";
var edges = [];
var users = [];
var posts = [];
var a = 0;

var generateNewsCategories = function() {
	return new Promise((resolve, reject) => {
		while ((line = readLines.next())) {
			try {
				const obj = JSON.parse(line);
				var headline = obj.headline;
				var category = obj.category;
				var authors = obj.authors;
				var link = obj.link;
				var description = obj.short_description;
				var date = obj.date;
				edges.push([headline, category, 2, 1]);
				posts.push([headline, category, authors, link, description, date]);
				a++;
			} catch (E) {
				continue;
			}
		}
		resolve();
	})
}

var getFriendsList = async function() {
	return new Promise((resolve, reject) => {
		var params = {
			TableName: "friends",
			ProjectionExpression: "idA, idB"
		}
		
		db.scan(params, function(err, data) {
			if (err) {
				console.log(err);
			} else {
				var value = data.Items;
				for (var i = 0; i < value.length; i++) {
					var userA = value[i].idA.S;
					var userB = value[i].idB.S;
					edges.push([userA, userB, 0, 0]);
				}
				resolve();
			}
		});
	})
}

var generateNews = async function() {
	var promise = generateNewsCategories(function(err, data) {
		if (err) {
			console.log("Error: " + err);
		}
	})
	
	await(promise.then(value => {}));
}

async function generateInterests() {
	return new Promise((resolve, reject) => {
		async function retrieveUserInterests() {
			var params = {
				TableName: "users",
				ProjectionExpression: "username, interests"
			}
			
			return new Promise((resolve, reject) => {
				var value;
				db.scan(params, function(err, data) {
					if (err) {
						console.log(err);
						reject();
					} else {
						value = data.Items;
						resolve(value);
					}
				});
			})
		}
		retrieveUserInterests().then(value => {
			for (var i = 0; i < value.length; i++) {
				var username = value[i].username.S;
				users.push(username);
				var interestList = value[i].interests.S.split(",");
				for (var j = 0; j < interestList.length; j++) {
					edges.push([username, interestList[j].replace(/ /g, ''), 0, 1]);
				}
			}
			resolve();
		})
	});
}

async function createTable() {
	fs.writeFile('newsSparkGraph.txt', '', function(){});
	async function initializeTable() {
		return new Promise((resolve, reject) => {
			initTable("news-spark", "id", function(err, data) {
				if (err) {
					console.log("Error initializing table: " + err);
				} else {
					resolve();
				}
			})
		})
	}
	
	initializeTable().then(() => {
		console.log("Generating edges:\n");
		console.log("Finished generating news\n");
		generateInterests().then(() => {
			console.log("Finished generating interests\n");
			getFriendsList().then(() => {
				console.log("Finished generating friends\n\nAdding edges:")
				var dataSend = []
				var batch = 0;
				/*
				for (var i = 0; i < edges.length;  i++) {
					var item = edges[i];
					for (var j = 0; j < item.length; j++) {
						dataSend.push(item[j]);
					}
					batch++;
					
					if (batch == 4) {
						async function add() {
							var promise = addEdge(dataSend, function(err, data) {
								if (err) {
									console.log("Error");
									console.log(err)
								}
							});
							promise.then(() => {
								console.log("Done");
								dataSend = [];
								batch = 0;
							});
						}
						add();
					}
				}
				
				if (dataSend.length != 0) {
					addEdge(dataSend, function(err, data) {
						if (err) {
							console.log(err)
						}
					})
				}
				*/
				
				console.log("Finished adding edges\nWriting to output text:\n");
				var outputText = "";
				var parser = "###";
				for (var i = 0; i < edges.length; i++) {
					var outputText = outputText + edges[i][2].toString() + edges[i][0] + parser + edges[i][3].toString() + edges[i][1] + parser + edges[i][2] + parser + edges[i][3] + "\n";
				}
				console.log("Writing to file:");
				fs.writeFile('newsSparkGraph.txt', outputText, (err) => {
					if (err) console.log(err);
				})
				console.log("count: " + a.toString());
			});
		});
	})
}

var addNewsEdge = async function(data) {
	return new Promise((resolve, reject) => {
		var headline = data[0];
		var category = data[1];
		var authors = data[2];
		var link = data[3];
		var description = data[4];
		var date = data[5];
		var params = {
			TableName: "news-posts",
			Item: {
				"id": {
					"S": headline
				}, "category": {
					"S": category
				}, "authors": {
					"S": authors
				}, "link": {
					"S": link
				}, "description": {
					"S": description
				}, "date": {
					"S": date
				}
			}
		}
		db.putItem(params, function(err, data) {
			if (err) {
				console.log(err);
				reject();
			} else {
				resolve();
			}
		})
	})
}

async function addNews() {
	return new Promise((resolve, reject) => {
		async function initializeTable() {
			return new Promise((resolve, reject) => {
				initTable("news-posts", "id", function(err, data) {
					if (err) {
						console.log("Error initializing table: " + err);
					} else {
						resolve();
					}
				})
			})
		}
		
		initializeTable().then(() => {
			async function addEdgesNews() {
				for (var i = 0; i < posts.length; i++) {
					console.log("Adding edge: " + i.toString() + "\n");
					await addNewsEdge(posts[i]);
				}
			}
			addEdgesNews();
			resolve();
		})
	})
}

generateNews();
addNews().then(() => {
	//createTable();
});
