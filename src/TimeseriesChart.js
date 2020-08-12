import React from 'react';
import Dygraph from 'dygraphs';
import 'dygraphs/dist/dygraph.css';
import useWindowSize from './useWindowSize';
const {useState, useEffect, useRef, useMemo, useCallback} = React;

// TODO: schema value types with formatters
const giga = 1024 * 1024 * 1024;

function TimeseriesChart({data, groupByCols}) {
  const chartEl = useRef(null);

  const windowSize = useWindowSize(300);

  useEffect(() => {
    if (data) {
      console.log(data);
      try {
        const colNames = data.data[0];
        const dsColName = colNames[0];
        const groupByDimensions = groupByCols.filter(
          (colName) => colName !== dsColName
        );

        // convert rows to objects
        let rows = data.data
          .slice(1)
          .filter((row) => row.length === colNames.length)
          .map((row) =>
            row.reduce((acc, val, index) => {
              acc[colNames[index]] = val;
              return acc;
            }, {})
          );

        // not sure about this
        rows = rows.filter((row) => row[dsColName] !== '');

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
          (c) => !(c === dsColName || groupByDimensions.includes(c))
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
  }, [data, groupByCols, windowSize]);

  return <div style={{width: '100%', height: '100%'}} ref={chartEl} />;
}

export default TimeseriesChart;
