import React from 'react';
import Papa from 'papaparse';
import DragAndDrop from './DragAndDrop';
const {useState, useEffect, useRef, useMemo, useCallback} = React;

function DropCSV() {
  const [data, setData] = useState(null);
  const handleDrop = useCallback(async (files) => {
    console.log(files);

    for (var i = 0; i < files.length; i++) {
      const file = await files[i].text();
      setData(Papa.parse(file));
      break;
    }
  }, []);
  return data ? (
    <Chart data={data} groupByCols={['operation']} />
  ) : (
    <DragAndDrop handleDrop={handleDrop}>
      <div style={{height: 100, padding: 16}}>
        <p>Drag one or more files to this Drop Zone ...</p>
      </div>
    </DragAndDrop>
  );
}
