var Parse = require('parse/node');
var inquirer = require('inquirer');

var schemas = require('./schemas');
var transfer = require('./transfer');
var questions = require('./questions.js');

module.exports = initialize;

function initialize(config, handler) {
  if (!config.exportedModule) {
    questions(config).then(function (answers) {
      config = Object.assign(config, answers);
      console.log(JSON.stringify(config, null, 2));
      return inquirer.prompt({
        type: 'confirm',
        name: 'next',
        message: 'About to start the file transfer. Does the above look correct?',
        default: true,
      });
    }).then(function (answers) {
      if (!answers.next) {
        console.log('Aborted!');
        process.exit();
      }
      Parse.initialize(config.applicationId, null, config.masterKey);
      Parse.serverURL = config.serverURL;
      return transfer.init(config);
    }).then(function () {
      return getAllFileObjects();
    }).then(function (objects) {
      return transfer.run(objects);
    }).then(function () {
      console.log('Complete!');
      process.exit();
    }).catch(function (error) {
      console.log(error);
      process.exit(1);
    });
  }
  else {
    Parse.initialize(config.applicationId, null, config.masterKey);
    Parse.serverURL = config.serverURL;
    return transfer.init(config)
        .then(function () {
          return getAllFileObjects();
        }).then(function (objects) {
          return transfer.run(objects, handler);
        }).catch(function (error) {
          console.log(error);
        });
  }
}

function getAllFileObjects() {
  console.log("Fetching schema...");
  return schemas.get().then(function(res){
    console.log("Fetching all objects with files...");
    var schemasWithFiles = onlyFiles(res);
    return Promise.all(schemasWithFiles.map(getObjectsWithFilesFromSchema));
  }).then(function(results) {
    var files = results.reduce(function(c, r) {
      return c.concat(r);
    }, []).filter(function(file) {
      return file.fileName !== 'DELETE';
    });

    return Promise.resolve(files);
  });
}

function onlyFiles(schemas) {
  return schemas.map(function(schema) {
    var fileFields = Object.keys(schema.fields).filter(function(key){
      var value = schema.fields[key];
      return value.type == "File" || value.type == "Array";
    });
    if (fileFields.length > 0) {
      return {
        className: schema.className,
        fields: fileFields
      }
    }
  }).filter(function(s){ return s != undefined })
}

function getAllObjects(baseQuery)  {
  var allObjects = [];
  var next = function() {
    if (allObjects.length) {
      baseQuery.greaterThan('createdAt', allObjects[allObjects.length-1].createdAt);
    }
    return baseQuery.find({useMasterKey: true}).then(function(r){
      allObjects = allObjects.concat(r);
      if (r.length == 0) {
        return Promise.resolve(allObjects);
      } else {
        return next();
      }
    });
  }
  return next();
}

// Cases covered:
// i   - It is a single File
// ii  - It is a single Array with no files
// iii - It is a single Array with only files
// iv  - There are multiple Arrays with only files
// v   - There are multiple Arrays with no files
// vi  - There are multiple Arrays with file and non-file elements

var ARRAY_WITH_FILES = 1;
var ARRAY_NO_FILES = 2;
var FILE = 3;

function check(element, schema) {
  var toRet = FILE;
  var hasArray = false;
  var hasFile = false;
  var data = [];
  var fields = schema.fields;


  fields.forEach(function(key) {
    if(Array.isArray(element.get(key))) {
      hasArray = true;
      element.get(key).forEach(function(item) {
        if(item instanceof Parse.File) {
          hasFile = true;
          var fName = item._name;
          var fUrl = item._url;
          data.push({
            className: schema.className,
            objectId: item.id,
            fieldName: key,
            fileName: fName,
            url: fUrl
          });
        }
      });
    }
  });

  if(hasArray && hasFile) toRet = ARRAY_WITH_FILES;
  else if(hasArray) toRet = ARRAY_NO_FILES;

  var returnObj = {
    code: toRet,
    data: data,
  };

  return returnObj;
}

function checkArrays(result, schema) {
  var status = check(result, schema);
  if(status.code === ARRAY_WITH_FILES) {
    return status.data
  }
  else if(status.code === ARRAY_NO_FILES) {
    return []
  }
  else {
    return schema.fields.map(function(field){
      var fName = result.get(field) ? result.get(field).name() : 'DELETE';
      var fUrl = result.get(field) ? result.get(field).url() : 'DELETE';
      return {
        className: schema.className,
        objectId: result.id,
        fieldName: field,
        fileName: fName,
        url: fUrl
      }
    })
  }
}

function getObjectsWithFilesFromSchema(schema) {
  var query = new Parse.Query(schema.className);
  query.select(schema.fields.concat('createdAt'));
  query.ascending('createdAt');
  query.limit(1000);

  var checks = schema.fields.map(function(field) {
      return new Parse.Query(schema.className).exists(field);
  });
  query._orQuery(checks);

  return getAllObjects(query).then(function(results) {
    return results.reduce(function(current, result){
      return current.concat(
        checkArrays(result, schema)
      );
    }, []);
  });
}
