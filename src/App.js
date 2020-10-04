import React from 'react';
import Papa from 'papaparse';
import io from 'socket.io-client';
import FuzzySelect from './FuzzySelect';
import Chart from './TimeseriesChart';

import athenaQuery from './athenaQuery';

const {useState, useEffect, useRef, useMemo, useCallback} = React;

var searchParams = new URLSearchParams(window.location.search);

if (!searchParams.has('port')) {
  window.alert(`'port' url query param required`);
  throw new Error(`'port' url query param required`);
}

const socketPort = parseInt(searchParams.get('port'));

const styles = {
  section: {margin: '8px 0'},
  sectionLabel: {marginBottom: 8, fontWeight: 'bold'},
  td: {
    maxWidth: 100,
    overflow: 'hidden',
  },
  th: {
    maxWidth: 100,
    backgroundColor: '#ccc',
  },
};

function serverURL(path) {
  return `http://localhost:${socketPort}${path}`;
}

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

  const onToggle = useCallback(
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

function useJSONRequest(uri) {
  const [data, setData] = useState(null);
  useEffect(() => {
    fetch(uri)
      .then((res) => res.json())
      .then((json) => {
        setData(json);
      })
      .catch((err) => console.error('failed to load', uri, err));
  }, [uri]);

  return data;
}

function QueryBuilder({api, schema}) {
  const [state, setState] = useState({
    aggCols: {},
    groupByCols: [],
    type: 'timeseries',
    orderBy: null,
  });

  function handleTypeChange(e) {
    const {name, checked} = e.currentTarget;
    setState((s) => ({
      ...s,
      type: e.currentTarget.value,
    }));
  }
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
      groupByCols: selected.map((option) => option.name),
    }));
  }
  function handleOrderByChange(orderBy) {
    setState((s) => ({
      ...s,
      orderBy: last(orderBy)?.id,
    }));
  }

  const aggCols = Object.entries(state.aggCols)
    .filter(([col, enabled]) => enabled)
    .map(([col, enabled]) => ({name: col}));
  const query = {
    groupByCols: (state.type === 'timeseries' ? ['ds'] : []).concat(
      state.groupByCols
    ),
    aggCols,
    cols: aggCols,
    defaultAgg: 'sum',
    filters: [],
    schema,
    type: state.type,
  };

  const groupBySelectValues = useMemo(
    () => state.groupByCols.map((col) => ({id: col, name: col})),
    [state.groupByCols]
  );

  const groupableColumns = useMemo(
    () =>
      Object.entries(schema.fields).filter(([col, f]) => f.type === 'string'),
    [schema.fields]
  );

  const groupBySelectOptions = useMemo(
    () => groupableColumns.map(([col, f]) => ({id: col, name: col})),
    [groupableColumns]
  );

  const orderBySelectOptions = useMemo(
    () =>
      Object.entries(schema.fields).map(([col, f]) => ({id: col, name: col})),
    [schema.fields]
  );
  const orderBySelectValue = useMemo(
    () => [{id: state.orderBy, name: state.orderBy}],
    [state.orderBy]
  );

  return (
    <div style={{minWidth: 400, maxWidth: 600, margin: 16}}>
      <h2>brodo</h2>
      <h3>{schema.table}</h3>
      <div style={styles.section}>
        <label style={styles.sectionLabel}>query type:</label>
        <select value={state.type} onChange={handleTypeChange}>
          {['table', 'samples', 'timeseries'].map((v) => (
            <option key={v} value={v}>
              {v}
            </option>
          ))}
        </select>
      </div>
      {state.type === 'samples' && (
        <div style={styles.section}>
          <div style={styles.sectionLabel}>columns:</div>
          {Object.entries(schema.fields).map(([col, f]) => (
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
      )}
      {state.type !== 'samples' && (
        <>
          <div style={styles.section}>
            <div style={styles.sectionLabel}>group by:</div>
            <FuzzySelect
              onChange={handleGroupByChange}
              value={groupBySelectValues}
              options={groupBySelectOptions}
              placeholderText="Add a group by column"
              removeButtonText="Click to remove column"
            />
            <Details summary={'groupable columns'}>
              {groupableColumns.map(([col, f]) => col).join(', ')}
            </Details>
          </div>
          <div style={styles.section}>
            <div style={styles.sectionLabel}>aggregate columns:</div>
            {Object.entries(schema.fields)
              .filter(([col, f]) => f.type === 'number')
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
        </>
      )}

      <div style={styles.section}>
        <div style={styles.sectionLabel}>order by:</div>
        <FuzzySelect
          onChange={handleOrderByChange}
          value={state.orderBy ? orderBySelectValue : []}
          options={orderBySelectOptions}
          placeholderText="set order by column"
          removeButtonText="Click to clear"
        />
      </div>
      <Details summary={'query debug'}>
        <pre>{JSON.stringify(state, null, 2)}</pre>
        <pre>
          {state.type === 'samples'
            ? athenaQuery.buildSampleQuery(query)
            : athenaQuery.buildAggQuery(query)}
        </pre>
      </Details>
      <button
        style={styles.section}
        disabled={query.aggCols.length === 0}
        onClick={() =>
          api.sendCommand('query', {
            sql:
              state.type === 'samples'
                ? athenaQuery.buildSampleQuery(query)
                : athenaQuery.buildAggQuery(query),
            query,
          })
        }
      >
        Run Query
      </button>
    </div>
  );
}

function resultURL(queryID) {
  return serverURL(`/query-result?id=${queryID}`);
}

function isQuerySuccessful(query) {
  return query?.state === 'SUCCEEDED';
}

function DataTable({data}) {
  return (
    <>
      <table>
        <tr>
          {data.data[0].map((colName) => (
            <th style={styles.th} key={colName}>
              {colName}
            </th>
          ))}
        </tr>
        {data.data.slice(1, 1000).map((row, i) => (
          <tr key={i}>
            {row.map((v, i) => (
              <td style={styles.td} key={i}>
                {v}
              </td>
            ))}
          </tr>
        ))}
      </table>

      {data.data.length > 1000 && (
        <div style={{backgroundColor: '#eee'}}>
          additional rows truncated ({data.data.length} rows total)
        </div>
      )}
    </>
  );
}

function LoadChart({dataURL, queryState}) {
  const [data, setData] = useState(null);
  useEffect(() => {
    loadCSV(dataURL).then((data) => {
      setData(data);
    });
  }, []);

  return (
    <div style={{height: '100%', display: 'flex', flexDirection: 'column'}}>
      {queryState.query.type === 'timeseries' ? (
        <div style={{flex: 1, minHeight: '50%'}}>
          <Chart data={data} groupByCols={queryState.query.groupByCols} />
        </div>
      ) : (
        data && <DataTable data={data} />
      )}
      <Details summary={<span>result</span>}>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Details>
    </div>
  );
}

function Errors({clientErrors, serverErrors}) {
  return (
    <div style={{backgroundColor: 'red', color: 'white'}}>
      {clientErrors.concat(serverErrors).map(({message, error}, i) => (
        <Details
          key={i}
          summary={
            message.length > 300 ? message.slice(0, 300) + '...' : message
          }
        >
          <pre>{message + '\n' + error}</pre>
        </Details>
      ))}
    </div>
  );
}

function App() {
  const schema = useJSONRequest(serverURL('/schema'));
  const [state, setState] = useState(null);
  const [clientErrors, setClientErrors] = useState([]);

  let apiRef = useRef(null);
  let api = apiRef.current;

  useEffect(() => {
    const socket = io.connect(serverURL(''));
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

  if (!schema) {
    return <div>loading...</div>;
  }

  return (
    <div style={{height: '100vh'}}>
      {state && (
        <Errors {...{clientErrors, serverErrors: state.serverErrors}} />
      )}
      <div style={{display: 'flex', height: '100%'}}>
        <div>
          <QueryBuilder api={api} schema={schema} />

          {state &&
            Object.entries(state.queryStates).map(([queryID, query], i) => (
              <Details
                key={i}
                summary={
                  <span>
                    query {i} ({query.state})
                  </span>
                }
              >
                <div style={{margin: 16}}>
                  <Details key={i} summary={<span>query metadata</span>}>
                    <pre>{JSON.stringify(query, null, 2)}</pre>
                  </Details>
                  <Details key={i} summary={<span>preview</span>}>
                    {isQuerySuccessful(query) && (
                      <LoadChart
                        dataURL={resultURL(queryID)}
                        queryState={query}
                      />
                    )}
                  </Details>
                </div>
              </Details>
            ))}
          {false && (
            <button
              onClick={() => {
                Object.entries(state.queryStates).forEach(
                  ([queryID, query]) => {
                    if (queryID) {
                      api.sendCommand('status', {
                        queryExecutionId: queryID,
                      });
                    }
                  }
                );
              }}
            >
              refresh queries
            </button>
          )}
        </div>

        <div style={{flex: '1', height: '100%'}}>
          {isQuerySuccessful(lastQuery?.[1]) && (
            <LoadChart
              dataURL={resultURL(lastQuery[0])}
              queryState={lastQuery[1]}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
