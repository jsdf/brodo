import React from 'react';
// import logo from './logo.svg';
// import './App.css';
import Dygraph from 'dygraphs';
import 'dygraphs/dist/dygraph.css';
import Papa from 'papaparse';
import DragAndDrop from './DragAndDrop';
import io from 'socket.io-client';

const {useState, useEffect, useRef} = React;

var searchParams = new URLSearchParams(window.location.search);

if (!searchParams.has('port')) {
  window.alert(`'port' url query param required`);
  throw new Error(`'port' url query param required`);
}

const socketPort = parseInt(searchParams.get('port'));

async function loadCSV(uri) {
  const res = await fetch(uri);
  const csvString = await res.text();
  debugger;
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

function Chart({data}) {
  const chartEl = React.useRef(null);

  React.useEffect(() => {
    if (data) {
      console.log(data);
      try {
        const groupByDimensions = ['operation'];
        const colNames = data.data[0];
        const dsColName = colNames[0];

        // convert rows to objects
        const rows = data.data
          .slice(1)
          .filter((row) => row.length == colNames.length)
          .map((row) =>
            row.reduce((acc, val, index) => {
              acc[colNames[index]] = val;
              return acc;
            }, {})
          );

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

        const labels = [dsColName].concat(unfoldedCols);

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

function LoadChart({dataURL}) {
  const [data, setData] = React.useState(null);
  React.useEffect(() => {
    loadCSV(dataURL).then((data) => {
      setData(data);
    });
  }, []);

  return <Chart data={data} />;
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

  return (
    <div>
      {false && (
        <details>
          <DropCSV />
          <summary>drag and drop data</summary>
        </details>
      )}
      {state &&
        Object.entries(state.queryStates).map(([queryID, query], i) => (
          <Details key={i} summary={<span>query {i}</span>}>
            <div style={{margin: 16}}>
              <Details key={i} summary={<span>query metadata</span>}>
                <pre>{JSON.stringify(query, null, 2)}</pre>
              </Details>
              <LoadChart
                dataURL={`http://localhost:${socketPort}/query-result?id=${queryID}`}
              />
            </div>
          </Details>
        ))}
      <button
        onClick={() => {
          api.sendCommand('status', {
            queryExecutionId: 'c93d2db5-3d25-4a9d-8be0-c94accf8f746',
          });
        }}
      >
        load from athena
      </button>
    </div>
  );
}

export default App;
