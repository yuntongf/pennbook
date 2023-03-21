var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var uuid = require('uuid');

/*
Function to retrieve a user A. Returns user A's information
Args:
	-user: user to look up
Returns: User A's information if they exist. Else, null or error
*/
var myDB_getUser = function(user, callback) {
	console.log("trying to getItem user: "+user);
	var params = {
		TableName: "users",
		Key: {"username": {
			"S": user
		}}
	};
	
	db.getItem(params, function(err, data) {
		try {
			if (data.Item == undefined) {
				console.log("returned Item is undefined");
				return null;
			} else {
				console.log("getUser was successfully executed for user: "+user);
				return data.Item;
			}
		} catch (err) {
			console.log(err);
			return err;
		}
	})
}

// different from myDB_getUserInfo because it uses the callback function
var myDB_getUserInfo = function(user, callback) {
	console.log("trying to getUserInfo user: "+user);
		var params = {
		TableName: "users",
		Key: {"username": {
			"S": user
		}}
	};
	
	db.getItem(params, function(err, data) {
		try {
			if (data.Item == undefined) {
				console.log("returned Item is undefined");
				callback(err,null);
			} else {
				console.log("getUserInfo was successfully executed for user: "+user);
				callback(err,data.Item);
			}
		} catch (err) {
			console.log(err);
			callback(err);
		}
	})
}

var myDB_getUsers = function(callback) {
	var params = {
		TableName: "users",
		ProjectionExpression: "username"
	}
	
	db.scan(params, function(err, data) {
		if (err) {
			console.log(err);
			callback(err, null);
		} else {
			callback(err, data.Items);
		}
	});
}

/*
Function to add friends A and B. It will create 2 new entries in the friends database: 
A to B and B to A. Assumes both user A and B exist.
Args:
	- userA: username of friend that sent the request
	- userB: username of friend that the request got sent to
	- callback: callback function
Returns: uniqueID (A/B) generated of the two friends. err otherwise
*/

var myDB_addFriend = function(userA, userB, callback) {
	var uniqueID1 = userA + "/" + userB;
	var uniqueID2 = userB + "/" + userA;
	var params1 = {
		TableName: "friends",
		Item: {
			"id": {
				"S": uniqueID1
			}, "idA": {
				"S": userA
			}, "idB": {
				"S": userB
			}
		}
	};
	
	var params2 = {
		TableName: "friends",
		Item: {
			"id": {
				"S": uniqueID2
			}, "idA": {
				"S": userB
			}, "idB": {
				"S": userA
			}
		}
	};
	
	db.putItem(params1, function(err, data) {
		if (err) {
			callback(err, null);
		} else {
			db.putItem(params2, function(err2, data2) {
				if (err2) {
					callback(err2, null);
				} else {
					callback(err2, uniqueID1);
				}
			})
		}
	})
}

/*
Function to remove friends A and B. It will remove A-B and B-A from the friends database.
Assumes both user A and B exist.
Args:
	- userA: username of friend that sent the remove request
	- userB: username of friend that the request got sent to
	- callback: callback function
Returns: null
*/
var myDB_deleteFriend = function(userA, userB, callback) {
	var uniqueID1 = userA + "/" + userB;
	var uniqueID2 = userB + "/" + userA;
	var params1 = {
		TableName: "friends",
		Key: {
			'id': {S: uniqueID1},
		}
	};
	
	var params2 = {
		TableName: "friends",
		Key: {
			'id': {S: uniqueID2},
		}
	}
	
	db.deleteItem(params1, function(err, data) {
		if (err) {
			callback(err, err);
		} else {
			db.deleteItem(params2, function(err2, data2) {
				if (err2) {
					callback(err2, err2);
				} else {
					callback(err2, "success");
				}
			})
		}
	});
}

/*
Checks friendship between user A and user B.
Args:
	- userA
	- userB
	- callback
Returns: True if friends, False if not
*/
var myDB_checkFriendship = function(userA, userB, callback) {
	var uniqueID = userA + "/" + userB;	
	var params = {
		TableName: "friends",
		Key: {"id": {
			"S": uniqueID
		}}
	};
	
	db.getItem(params, function(err, data) {
		try {
			if (data.Item == undefined) {
				callback(err, false);
			} else {
				callback(err, true);
			}
		} catch (err) {
			console.log(err);
			callback(err, false);
		}
	})
}

var myDB_getFriends = function(user, callback) {
	var params = {
		TableName: "friends",
		ProjectionExpression: "idB, firstNameB, lastNameB, affiliationB",
		ExpressionAttributeValues: {
			":a": {
				S: user
			}
		},
		FilterExpression: "idA = :a"
	}
	
	db.scan(params, function(err, data) {
		if (err) {
			console.log(err);
			callback(err, null);
		} else {
			callback(err, data);
		}
	});
}

var myDB_checklogin = function(username, callback) {
  console.log('Looking up: ' + username); 

  var params = {
      KeyConditions: {
        username: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: username }]
        }
      },
      TableName: "users",
      AttributesToGet: [ 'password' ]
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
	  console.log("error: "+err);
      callback(err, null);
    } else {
      callback(err, data.Items[0].password.S);
    }
  });
}

var myDB_createAccount = function(username, hashed_pw, firstname, lastname, email, 
			affiliation, birthday, interests, callback) {
  console.log('Trying to create account: ' + username); 
  
  var params = {
    TableName: "users",
    Item: {
        "username": {
          	S: username
        },
        "password": { 
          	S: hashed_pw
        },
        "firstname": { 
          	S: firstname
        },
        "lastname": {
		  S: lastname
		},
		"email": {
		  S: email
		},
		"affiliation": {
		  S: affiliation
		},
		"birthday": {
		  S: birthday
		},
		"interests": {
		  S: interests
		}
    },
    ConditionExpression: 'attribute_not_exists(username)',
    ReturnValues: 'NONE'
  };
  
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
}

var myDB_retrieveProfile = function(username, callback) {
  console.log('Retrieving profile: ' + username); 

  var params = {
      Key: {
		"username": {
          	S: username
        },
  	  }, 
      TableName: "users",
  };

  db.getItem(params, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(err, data.Item);
    }
  });
}

var myDB_updateProfile = function(username, email, password, affiliation, interests, callback) {
	console.log("updating profile: " + username); 
	var params = {
  		ExpressionAttributeNames: {
   			"#EM": "email", 
   			"#PW": "password",
   			"#AF": "affiliation",
   			"#IN": "interests"
  		}, 
 	 	ExpressionAttributeValues: {
   			":e": {
     			S: email
    		}, 
    		":p": {
     			S: password
    		}, 
    		":a": {
     			S: affiliation
    		}, 
    		":i": {
     			S: interests
    		}, 
  		}, 
 		Key: {
			"username": {
          		S: username
        	},
  	  	}, 
 		ReturnValues: "ALL_NEW", 
  		TableName: "users", 
  		UpdateExpression: "SET #EM = :e, #PW = :p, #AF = :a, #IN = :i"
 	};

    db.updateItem(params, function(err, data) {
        if (err) {
      		callback(err, null);
    	} else {
      		callback(err, data.Attributes);
    	}
    });
}

var myDB_retrieveWall = function(username, callback) {
  console.log('Getting wall: ' + username); 
  
  	var params = {
		TableName: "posts",
		ProjectionExpression: "id, datatime, poster_id, content",
		ExpressionAttributeValues: {
			":w": {
				S: username
			}
		},
		FilterExpression: "walluser_id = :w"
	}
	
	db.scan(params, function(err, data) {
		if (err) {
			console.log(err);
			callback(err, null);
		} else {
			callback(err, data);
		}
	});
}

var myDB_retrievePosts = function(username, callback) {
  console.log('Getting posts: ' + username); 

  var params = {
		TableName: "posts",
		ProjectionExpression: "id, datatime, content",
		ExpressionAttributeValues: {
			":p": {
				S: username
			}
		},
		FilterExpression: "poster_id = :p"
	}
	
	db.scan(params, function(err, data) {
		if (err) {
			console.log(err);
			callback(err, null);
		} else {
			callback(err, data);
		}
	});
}

var myDB_retrieveComments = function(post_id, callback) {
	console.log('Getting comments: ' + post_id); 
  
  var params = {
		TableName: "comments",
		ProjectionExpression: "datatime, commenter_id, content",
		ExpressionAttributeValues: {
			":p": {
				S: post_id
			}
		},
		FilterExpression: "post_id = :p"
	}
	
	db.scan(params, function(err, data) {
		if (err) {
			console.log(err);
			callback(err, null);
		} else {
			callback(err, data);
		}
	});
}

var myDB_postOnWall = function(poster_id, walluser_id, content, callback) {
  console.log("posting on " + walluser_id + "'s wall'");
  var params = {
    TableName: "posts",
    Item: {
        "id": {
          	S: uuid.v1()
        },
        "datatime": { 
          	N: Date.now().toString()
        },
        "poster_id": { 
          	S: poster_id
        },
        "walluser_id": {
		  S: walluser_id
		},
		"content": {
		  S: content
		}
    },
    ConditionExpression: 'attribute_not_exists(id)',
    ReturnValues: 'NONE'
  };
  
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
}

var myDB_commentOnPost = function(commenter_id, post_id, content, callback) {
  console.log("Commenting on " + post_id);
  var params = {
    TableName: "comments",
    Item: {
        "id": {
          	S: uuid.v1()
        },
        "datatime": { 
          	N: Date.now().toString()
        },
        "commenter_id": { 
          	S: commenter_id
        },
        "post_id": {
		  S: post_id
		},
		"content": {
		  S: content
		}
    },
    ConditionExpression: 'attribute_not_exists(id)',
    ReturnValues: 'NONE'
  };
  
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });	
}

var myDB_getOnline = function(username, callback) {
	var params = {
    TableName: "online_users",
    Item: {
        "username": {
          	S: username
        },
		"online": {
		  S: 'Yes'
		}
    },
    ConditionExpression: 'attribute_not_exists(id)',
    ReturnValues: 'NONE'
  };
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });	
}

var myDB_getOffline = function(username, callback) {
	var params = {
    TableName: 'online_users',
    Key: {
      'username': {S: username},
      'online': {S: 'Yes'}
    }
  };

  db.deleteItem(params, function(err, data) {
    if (err) {
      console.log("Error", err);
      callback(err, null);
    } else {
      console.log("Success", data);
      callback(err, data);
    }
  });	
}

var myDB_getOnlineUsers = function(callback) {
	var params = {
  		TableName : 'online_users',
  		ProjectionExpression: 'username'
	};
	
	db.scan(params, function(err, data) {
  		if (err) {
      		console.log("Error", err);
     	 	callback(err, null);
   		} else {
      		//console.log("Success", data);
      		callback(err, data.Items);
    	}
	});

}

//send list of posts to get full details
var myDB_getNewsFeed = async function(headline, callback) {
	return new Promise((resolve, reject) => {
		var params = {
			TableName: 'news-posts',
			Key: {"id": {
				"S": headline
			}}
		}
		
		db.getItem(params, function(err, data) {
			try {
				if (data.Item == undefined) {
					resolve();
				} else {
					resolve(data.Item);
				}
			} catch (err) {
				reject();
			}
		})
	})
}

var database = { 
  getUser: myDB_getUser,
  getUserInfo: myDB_getUserInfo,
  getUsers: myDB_getUsers,
  addFriend: myDB_addFriend,
  deleteFriend: myDB_deleteFriend,
  checkFriendship: myDB_checkFriendship,
  getFriends: myDB_getFriends,
  check_login: myDB_checklogin,
  create_account: myDB_createAccount,
  retrieve_profile: myDB_retrieveProfile,
  update_profile: myDB_updateProfile,
  retrieve_wall: myDB_retrieveWall,
  retrieve_comments: myDB_retrieveComments,
  postOnWall: myDB_postOnWall,
  commentOnPost: myDB_commentOnPost,
  retrieve_posts: myDB_retrievePosts,
  getOnline: myDB_getOnline,
  getOffline: myDB_getOffline,
  getOnlineUsers: myDB_getOnlineUsers,
  getNewsFeed: myDB_getNewsFeed
};

module.exports = database;