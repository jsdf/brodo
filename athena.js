var AWS = require('aws-sdk');

function init({region}) {
  AWS.config.region = region;

  var s3 = new AWS.S3();
  var athena = new AWS.Athena();

  function listQueryExecutions() {
    var params = {
      // MaxResults: 100,
      // NextToken: 'STRING_VALUE',
      // WorkGroup: 'STRING_VALUE'
    };
    return athena.listQueryExecutions(params).promise();
  }

  function runQuery({bucket, outputLocation}, queryString) {
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

  function getTableMetadata(dbAndTable) {
    const [db, table] = dbAndTable.split('.');
    var params = {
      CatalogName: 'AwsDataCatalog' /* required */,
      DatabaseName: db /* required */,
      TableName: table /* required */,
    };
    return athena.getTableMetadata(params).promise();
  }

  async function getSchemaFields(dbAndTable) {
    const metadata = await getTableMetadata(dbAndTable);

    const fields = {};
    const colTypeMapping = {
      bigint: 'number',
    };
    metadata.TableMetadata.Columns.forEach((col) => {
      fields[col.Name] = {type: colTypeMapping[col.Type] || col.Type};
    });
    return fields;
  }

  function getObject({bucket}, key) {
    var params = {
      Bucket: bucket,
      Key: key,
    };
    return s3.getObject(params).promise();
  }

  async function getQueryResultsCSV(
    {bucket, outputLocation},
    queryExecutionId
  ) {
    const res = await getObject(`${outputLocation}/${queryExecutionId}.csv`);
    return res.Body.toString();
  }

  return {
    runQuery,
    getQueryStatus,
    getQueryResultsCSV,
    getSchemaFields,
  };
}

module.exports = {
  init,
};
