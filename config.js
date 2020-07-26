module.exports = {
  port: 13337,
  athena: {
    region: 'us-east-1',
    bucket: 'jfriend-logs',
    outputLocation: 'athena-queries/brodo',

    schema: {
      table: 's3_access_logs_db.jfriend_logs',
      timeCol: 'ds',
      fields: {
        // this field is needed for the time series graph
        ds: {
          type: 'string',
          derived: `regexp_extract(requestdatetime, '^(.*?):', 1)`,
        },
        transfer: {type: 'number', derived: `bytessent + objectsize`},
        // other fields are automatically determined from athena api
      },
    },
  },
};
