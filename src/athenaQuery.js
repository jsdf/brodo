function buildAgg(col, aggType) {
  switch (aggType) {
    case 'p75':
      return `approx_percentile(${col}, 0.75)`;
    case 'p90':
      return `approx_percentile(${col}, 0.90)`;
    case 'p95':
      return `approx_percentile(${col}, 0.95)`;
    case 'p99':
      return `approx_percentile(${col}, 0.99)`;
    default:
      return `${aggType}(${col})`;
  }
}

function range(startAt, size) {
  return [...Array(size).keys()].map((i) => i + startAt);
}

function t(type, childType) {
  return {type, childType};
}

function valueToSQLLiteral(value, type) {
  switch (typeof type == 'string' ? type : type.type) {
    case 'tuple':
      return `(${value
        .map((v) => valueToSQLLiteral(v, type.childType))
        .join(', ')})`;
    case 'array':
      return `ARRAY [${value
        .map((v) => valueToSQLLiteral(v, type.childType))
        .join(', ')}]`;
    case 'string':
      return `'${value}'`;
    default:
      return String(value);
  }
}

function buildAggQuery({
  groupByCols /*: Array<string>*/,
  aggCols /*: Array<{name: string, agg?: string}> */,
  defaultAgg /*:string*/,
  filters /*: {col: string, op: string, value: mixed, valueType?: string}*/,
  schema /*: Schema */,
}) {
  const selects = [];
  groupByCols.forEach((col) => {
    selects.push(
      schema.fields[col]?.derived != null
        ? `${schema.fields[col].derived} as ${col}`
        : col
    );
  });
  aggCols.forEach((col) => {
    const agg = col.agg || defaultAgg;
    selects.push(
      `${buildAgg(
        schema.fields[col.name]?.derived != null
          ? schema.fields[col.name].derived
          : col.name,
        agg
      )} as ${col.name}_${agg}_agg`
    );
  });

  const wheres = filters.map(
    (f) => `${f.col} ${f.op} ${valueToSQLLiteral(f.value, f.valueType)}`
  );

  return `
SELECT 
${selects.join(',\n')}
FROM ${schema.table}
${wheres.length ? `WHERE ${wheres.join('\n AND ')}` : ''}
GROUP BY ${range(1, groupByCols.length).join(', ')}
    `;
}

function buildSampleQuery({cols, filters, schema}) {
  const selects = [];
  cols.forEach((col) => {
    selects.push(
      schema.fields[col]?.derived != null
        ? `${schema.fields[col].derived} as ${col}`
        : col
    );
  });

  const wheres = filters.map(
    (f) => `${f.col} ${f.op} ${valueToSQLLiteral(f.value, f.valueType)}`
  );

  return `
      SELECT 
        ${selects.join(',\n')}
      FROM ${schema.table}
      WHERE ${wheres.join('\n AND ')} 
    `;
}

module.exports = {
  buildSampleQuery,
  buildAggQuery,
};
