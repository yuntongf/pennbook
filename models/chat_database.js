var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var uuid = require('uuid');

var get_chatrooms = function(username, callback) {
  console.log('Getting chatrooms for: ' + username); 

  var params = {
      KeyConditions: {
        user_id: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: username }]
        }
      },
      TableName: "user_chats",
      AttributesToGet: [ 'chat_id' ]
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items);
    }
  });
}

var get_invites = function(username, callback) {
  console.log('Getting invites for: ' + username); 

  var params = {
      KeyConditions: {
        user_id: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: username }]
        }
      },
      TableName: "chat_invites"
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
	  console.log(data.Items);
      callback(err, data.Items);
    }
  });
}

var get_messages = function(chat_id, callback) {
  console.log('Getting messages for: ' + chat_id); 

  var params = {
      KeyConditions: {
        chat_id: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: chat_id }]
        }
      },
      TableName: "chats"
  };

  db.query(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items);
    }
  });
}

var add_invite = function(inviter, invited, chat_id, callback) {
  console.log("Adding invite for: " + invited);
  var params = {
    TableName: "chat_invites",
    Item: {
        "user_id": {
          	S: invited
        },
        "inviter": { 
          	S: inviter
        },
        "invite_chat": { 
          	S: chat_id
        }
    },
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

var accept_gc_invite = function(username, chat_id, callback) {
  console.log("Accepting invite to gc: " + chat_id);
  var params = {
    TableName: "chatrooms",
    Item: {
        "chat_id": {
          	S: chat_id
        },
        "member": { 
          	S: username
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  	
  var params1 = {
    TableName: "user_chats",
    Item: {
        "user_id": {
          	S: username
        },
        "chat_id": { 
          	S: chat_id
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params1, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  
  //delete invite!
  var params2 = {
    TableName: 'chat_invites',
    Key: {
      'user_id': {S: username},
      'invite_chat': {S: chat_id}
    }
  };

  db.deleteItem(params2, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data);
    }
  });
}

var accept_pc_invite = function(username, inviter, chat_id, callback) {
  console.log("Accepting invite to pc: " + chat_id);
  var params = {
    TableName: "chatrooms",
    Item: {
        "chat_id": {
          	S: chat_id
        },
        "member": { 
          	S: username
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  	
  var params1 = {
    TableName: "user_chats",
    Item: {
        "user_id": {
          	S: username
        },
        "chat_id": { 
          	S: chat_id
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params1, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  
  //delete invite!
  var params2 = {
    TableName: 'chat_invites',
    Key: {
      'user_id': {S: username},
      'invite_chat': {S: chat_id}
    }
  };

  db.deleteItem(params2, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data);
    }
  });
  
  var params3 = {
    TableName: "chatrooms",
    Item: {
        "chat_id": {
          	S: chat_id
        },
        "member": { 
          	S: inviter
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params3, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  	
  var params4 = {
    TableName: "user_chats",
    Item: {
        "user_id": {
          	S: inviter
        },
        "chat_id": { 
          	S: chat_id
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params4, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
}

var accept_2_invite = function(invited_user, new_chat, old_invite_chat, user1, user2, callback) {
	console.log("accepting invite to create new group of 3");
	var params = {
    TableName: "chatrooms",
    Item: {
        "chat_id": {
          	S: new_chat
        },
        "member": { 
          	S: invited_user
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  	
  var params1 = {
    TableName: "user_chats",
    Item: {
        "user_id": {
          	S: invited_user
        },
        "chat_id": { 
          	S: new_chat
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params1, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  
  var params2 = {
    TableName: "chatrooms",
    Item: {
        "chat_id": {
          	S: new_chat
        },
        "member": { 
          	S: user1
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params2, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  	
  var params3 = {
    TableName: "user_chats",
    Item: {
        "user_id": {
          	S: user1
        },
        "chat_id": { 
          	S: new_chat
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params3, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  
  var params4 = {
    TableName: "chatrooms",
    Item: {
        "chat_id": {
          	S: new_chat
        },
        "member": { 
          	S: user2
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params4, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  	
  var params5 = {
    TableName: "user_chats",
    Item: {
        "user_id": {
          	S: user2
        },
        "chat_id": { 
          	S: new_chat
        }
    },
    ReturnValues: 'NONE'
  };
  
  db.putItem(params5, function(err, data) {
    if (err) {
       callback(err, null);
    } else {
       callback(err, data);
    }
  });
  
  var params6 = {
    TableName: 'chat_invites',
    Key: {
      'user_id': {S: invited_user},
      'invite_chat': {S: old_invite_chat}
    }
  };

  db.deleteItem(params6, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data);
    }
  });
  
}

var reject_invite = function(username, chat_id, callback) {
  console.log("Rejecting invite to: " + chat_id);
  var params = {
    TableName: 'chat_invites',
    Key: {
      'user_id': {S: username},
      'invite_chat': {S: chat_id}
    }
  };

  db.deleteItem(params, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data);
    }
  });	
}

var add_message = function(msg_id, chat_id, sender, time, content, callback) {
  var params = {
    TableName: "chats",
    Item: {
        "msg_id": {
          	S: msg_id
        },
        "chat_id": {
          	S: chat_id
        },
        "sender": { 
          	S: sender
        },
        "datatime": { 
          	N: time
        },
        "content": { 
          	S: content
        },
    },
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

var remove_user_from_chat = function(username, chat_id, callback) {
  var params = {
    TableName: 'chat_invites',
    Key: {
      'user_id': {S: username},
      'invite_chat': {S: chat_id}
    }
  };

  db.deleteItem(params, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data);
    }
  });	
}

var database = {
	getChatrooms: get_chatrooms,
	getInvites: get_invites,
	getMessages: get_messages,
	addInvite: add_invite,
	acceptGCInvite: accept_gc_invite, //existing member > 2
	acceptPCInvite: accept_pc_invite, //private chat, chat nonexistent until acception
	accept2Invite: accept_2_invite, // existing member = 2, create new gc upon acceptance
	rejectInvite: reject_invite,
	addMessage: add_message,
	removeUser: remove_user_from_chat,
}

module.exports = database;