import React from 'react';
// import logo from './logo.svg';
// import './App.css';
import Dygraph from 'dygraphs';
import 'dygraphs/dist/dygraph.css';
import Papa from 'papaparse';
import DragAndDrop from './DragAndDrop';
import io from 'socket.io-client';
import Select from 'react-select';

import athenaQuery from './athenaQuery';

const {useState, useEffect, useRef} = React;

var searchParams = new URLSearchParams(window.location.search);

if (!searchParams.has('port')) {
  window.alert(`'port' url query param required`);
  throw new Error(`'port' url query param required`);
}

const socketPort = parseInt(searchParams.get('port'));

function last(arr) {
  return arr[arr.length - 1];
}

async function loadCSV(uri) {
  const res = await fetch(uri);
  const csvString = await res.text();
  return Papa.parse(csvString);
}

function Details({summary, children}) {
  const [open, setOpen] = useState(false);

  const onToggle = React.useCallback(
    (e) => {
      e.preventDefault();
      e.stopPropagation();
      setOpen(!open);
    },
    [open]
  );

  return (
    <details open={open}>
      <summary onClick={onToggle}>{summary}</summary>
      {open && children}
    </details>
  );
}

const giga = 1024 * 1024 * 1024;

const schema = {
  table: 's3_access_logs_db.jfriend_logs',
  timeCol: 'ds',
  fields: {
    ds: {
      type: 'string',
      derived: `regexp_extract(requestdatetime, '^(.*?):', 1)`,
    },
    transfer: {type: 'number', derived: `bytessent + objectsize`},

    bucketowner: {type: 'string'},
    bucket: {type: 'string'},
    requestdatetime: {type: 'string'},
    remoteip: {type: 'string'},
    requester: {type: 'string'},
    requestid: {type: 'string'},
    operation: {type: 'string'},
    key: {type: 'string'},
    requesturi_operation: {type: 'string'},
    requesturi_key: {type: 'string'},
    requesturi_httpprotoversion: {type: 'string'},
    httpstatus: {type: 'string'},
    errorcode: {type: 'string'},
    bytessent: {type: 'number'},
    objectsize: {type: 'number'},
    totaltime: {type: 'string'},
    turnaroundtime: {type: 'string'},
    referrer: {type: 'string'},
    useragent: {type: 'string'},
    versionid: {type: 'string'},
    hostid: {type: 'string'},
    sigv: {type: 'string'},
    ciphersuite: {type: 'string'},
    authtype: {type: 'string'},
    endpoint: {type: 'string'},
    tlsversion: {type: 'string'},
  },
};

function QueryBuilder({api}) {
  const [state, setState] = useState({aggCols: {}, groupByCols: ['ds']});

  function handleAggColToggle(e) {
    const {name, checked} = e.currentTarget;
    setState((s) => ({
      ...s,
      aggCols: {...s.aggCols, [name]: checked},
    }));
  }
  function handleGroupByChange(selected) {
    setState((s) => ({
      ...s,
      groupByCols: selected ? selected.map((option) => option.value) : [],
    }));
  }
  const query = {
    groupByCols: state.groupByCols,
    aggCols: Object.entries(state.aggCols)
      .filter(([col, enabled]) => enabled)
      .map(([col, enabled]) => ({name: col})),
    defaultAgg: 'sum',
    filters: [],
    schema,
  };

  return (
    <div style={{maxWidth: 400}}>
      <h2>Query</h2>
      <div>
        group by:
        <Select
          onChange={handleGroupByChange}
          isMulti={true}
          options={Object.entries(schema.fields)
            .filter(([col, f]) => f.type == 'string')
            .map(([col, f]) => ({value: col, label: col}))}
        />
      </div>
      <div>
        aggregate columns:
        {Object.entries(schema.fields)
          .filter(([col, f]) => f.type == 'number')
          .map(([col, f]) => (
            <div
              key={col}
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                width: '100%',
                margin: 4,
              }}
            >
              <label htmlFor={col}>{col}: </label>
              <input
                name={col}
                type="checkbox"
                checked={Boolean(state.aggCols[col])}
                onChange={handleAggColToggle}
              />
            </div>
          ))}
      </div>
      <Details summary={'query debug'}>
        <pre>{JSON.stringify(state, null, 2)}</pre>
        <pre>{athenaQuery.buildAggQuery(query)}</pre>
      </Details>
      <button
        disabled={query.aggCols.length == 0}
        onClick={() =>
          api.sendCommand('query', {
            sql: athenaQuery.buildAggQuery(query),
            query,
          })
        }
      >
        Run Query
      </button>
    </div>
  );
}

function Chart({data, queryState}) {
  const chartEl = React.useRef(null);

  React.useEffect(() => {
    if (data) {
      console.log(data);
      try {
        const colNames = data.data[0];
        const dsColName = colNames[0];
        const groupByDimensions = queryState.query.groupByCols.filter(
          (colName) => colName != dsColName
        );

        // convert rows to objects
        let rows = data.data
          .slice(1)
          .filter((row) => row.length == colNames.length)
          .map((row) =>
            row.reduce((acc, val, index) => {
              acc[colNames[index]] = val;
              return acc;
            }, {})
          );

        // not sure about this
        rows = rows.filter((row) => row[dsColName] != '');

        const groupByDimensionsValues = {};
        groupByDimensions.forEach((dim) => {
          rows.forEach((row) => {
            groupByDimensionsValues[dim] =
              groupByDimensionsValues[dim] || new Set();
            groupByDimensionsValues[dim].add(row[dim]);
          });
        });

        const groupedByDS = {};
        rows.forEach((row) => {
          if (!(dsColName in row)) {
            throw new Error('ds column missing in row ' + JSON.stringify(row));
          }
          const ds = row[dsColName];
          groupedByDS[ds] = groupedByDS[ds] || [];
          groupedByDS[ds].push(row);
        });

        // the metric values: cols which are neither the ds or group by cols
        const valueCols = colNames.filter(
          (c) => !(c == dsColName || groupByDimensions.includes(c))
        );

        const timeseriesRecords = Object.entries(groupedByDS).map(
          ([ds, rowsgroup]) => {
            // wide rows with derived columns per group-by dimension
            const valueColValues = {};
            rowsgroup.forEach((row) => {
              valueCols.forEach((vcol) => {
                const unfoldedColKey =
                  groupByDimensions.map((dim) => row[dim]).join(' ') +
                  ' ' +
                  vcol;
                valueColValues[unfoldedColKey] = row[vcol];
              });
            });

            return [ds, valueColValues];
          }
        );

        const unfoldedCols = Object.keys(timeseriesRecords[0][1]);
        const timeseriesRows = timeseriesRecords
          .map(([ds, row]) =>
            [new Date(ds)].concat(
              unfoldedCols.map((col) => Number(row[col]) / giga)
            )
          )
          .sort((a, b) => a[0] - b[0]);

        const labels = [dsColName]
          .concat(unfoldedCols)
          .map((label) => label.replace(/_([^_]+)_agg$/, ' $1'));

        console.log(labels);

        console.log(timeseriesRows);

        new Dygraph(
          chartEl.current,
          timeseriesRows,

          {
            labels: labels,
            legend: 'always',
            title: 's3 traffic',
            showRoller: true,
            // rollPeriod: 14,
            // customBars: true,
            ylabel: 'gb',
          }
        );
      } catch (err) {
        console.error(err);
        debugger;
      }
    }
  }, [data]);

  return (
    <div
      style={{width: '100%', height: '80vh'}}
      ref={chartEl}
      className="App"
    />
  );
}

function resultURL(queryID) {
  return `http://localhost:${socketPort}/query-result?id=${queryID}`;
}

function isQuerySuccessful(query) {
  return query?.status?.QueryExecution.Status.State == 'SUCCEEDED';
}

function LoadChart({dataURL, queryState}) {
  const [data, setData] = React.useState(null);
  React.useEffect(() => {
    loadCSV(dataURL).then((data) => {
      setData(data);
    });
  }, []);

  return (
    <div>
      <Chart data={data} queryState={queryState} />
      <Details summary={<span>result</span>}>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Details>
    </div>
  );
}

function DropCSV() {
  const [data, setData] = React.useState(null);
  const handleDrop = React.useCallback(async (files) => {
    console.log(files);

    for (var i = 0; i < files.length; i++) {
      const file = await files[i].text();
      setData(Papa.parse(file));
      break;
    }
  }, []);
  return data ? (
    <Chart data={data} />
  ) : (
    <DragAndDrop handleDrop={handleDrop}>
      <div style={{height: 100, padding: 16}}>
        <p>Drag one or more files to this Drop Zone ...</p>
      </div>
    </DragAndDrop>
  );
}

function App() {
  const [state, setState] = useState(null);
  const [clientErrors, setClientErrors] = useState([]);
  const [logItems, setLogItems] = useState([]);

  let apiRef = useRef(null);
  let api = apiRef.current;

  useEffect(() => {
    const socket = io.connect(`http://localhost:${socketPort}`);
    socket.on('state', (newState) => {
      console.log(newState);
      setState(newState);
    });
    socket.on('disconnect', () => {
      console.log('got disconnect message');
      setClientErrors((prev) =>
        prev.concat({
          message: 'disconnected',
          error: null,
        })
      );
    });
    socket.on('error', (error) => {
      console.log('got error message', error);
      setClientErrors((prev) =>
        prev.concat({
          message: 'io error',
          error: error,
        })
      );
    });

    apiRef.current = {
      sendCommand(cmd, data) {
        socket.emit('cmd', {cmd, data});
      },
    };
  }, []);

  const lastQuery = state ? last(Object.entries(state.queryStates)) : null;

  return (
    <div>
      {false && (
        <details>
          <DropCSV />
          <summary>drag and drop data</summary>
        </details>
      )}
      <QueryBuilder api={api} />
      {state &&
        Object.entries(state.queryStates).map(([queryID, query], i) => (
          <Details
            key={i}
            summary={
              <span>
                query {i} ({query?.status?.QueryExecution.Status.State})
              </span>
            }
          >
            <div style={{margin: 16}}>
              <Details key={i} summary={<span>query metadata</span>}>
                <pre>{JSON.stringify(query, null, 2)}</pre>
              </Details>
              {isQuerySuccessful(query) && (
                <LoadChart dataURL={resultURL(queryID)} queryState={query} />
              )}
            </div>
          </Details>
        ))}

      {isQuerySuccessful(lastQuery?.[1]) && (
        <LoadChart
          dataURL={resultURL(lastQuery[0])}
          queryState={lastQuery[1]}
        />
      )}
      <button
        onClick={() => {
          Object.entries(state.queryStates).forEach(([queryID, query]) => {
            if (queryID) {
              api.sendCommand('status', {
                queryExecutionId: queryID,
              });
            }
          });
        }}
      >
        refresh queries
      </button>
    </div>
  );
}

export default App;
