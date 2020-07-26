var AWS = require('aws-sdk');

AWS.config.region = 'us-east-1';

var s3 = new AWS.S3();
var athena = new AWS.Athena();

const bucket = 'jfriend-logs';
const outputLocation = 'athena-queries/brodo';

function listQueryExecutions() {
  var params = {
    // MaxResults: 100,
    // NextToken: 'STRING_VALUE',
    // WorkGroup: 'STRING_VALUE'
  };
  return athena.listQueryExecutions(params).promise();
}

function runQuery(queryString) {
  var params = {
    QueryString: queryString /* required */,
    // ClientRequestToken: 'STRING_VALUE',
    // QueryExecutionContext: {
    //   Catalog: 'STRING_VALUE',
    //   Database: 'STRING_VALUE'
    // },
    ResultConfiguration: {
      //   EncryptionConfiguration: {
      //     EncryptionOption: SSE_S3 | SSE_KMS | CSE_KMS, /* required */
      //     KmsKey: 'STRING_VALUE'
      //   },
      OutputLocation: `s3://${bucket}/${outputLocation}`,
    },
    // WorkGroup: 'STRING_VALUE'
  };
  return athena.startQueryExecution(params).promise();
}

function getQueryStatus(queryExecutionId) {
  var params = {
    QueryExecutionId: queryExecutionId /* required */,
  };
  return athena.getQueryExecution(params).promise();
}

function getQueryResults(queryExecutionId) {
  var params = {
    QueryExecutionId: queryExecutionId /* required */,
    // MaxResults: 'NUMBER_VALUE',
    // NextToken: 'STRING_VALUE',
  };
  return athena.getQueryResults(params).promise();
}

function getObject(key) {
  var params = {
    Bucket: bucket,
    Key: key,
  };
  return s3.getObject(params).promise();
}

// const queryExecutionId = 'c93d2db5-3d25-4a9d-8be0-c94accf8f746';
// runQuery();
// getQueryStatus(queryExecutionId);
// getQueryResults(queryExecutionId);

async function getQueryResultsCSV(queryExecutionId) {
  const res = await getObject(`${outputLocation}/${queryExecutionId}.csv`);
  return res.Body.toString();
}

module.exports = {
  runQuery,
  getQueryStatus,
  getQueryResultsCSV,
};
