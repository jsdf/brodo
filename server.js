#!/usr/bin/env node

const http = require('http');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const spawn = require('child_process').spawn;
const getPort = require('get-port');
const express = require('express');
const socketio = require('socket.io');
const fs = require('fs');
const path = require('path');

const config = require('./config');
const athena = require('./athena').init(config.athena);

const DEV = true || process.env.NODE_ENV === 'development';

const uiRoot = 'dist';

const app = express();
const server = http.Server(app);
const io = socketio(server);
app.use(express.static(uiRoot));

function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

class ResultCache {
  methodsCache = new Map();

  // usage: `resultCache.get(obj.method.bind(obj), arg1, arg2)`
  // returns method result memoized by arguments so they are cached
  // for the lifetime of the server
  get(context, getter, ...args) {
    // cache for method
    const cacheByArgs = this.methodsCache.get(getter) || new Map();
    this.methodsCache.set(getter, cacheByArgs);

    const key = JSON.stringify(args);
    if (cacheByArgs.has(key)) {
      return cacheByArgs.get(key);
    }

    const result = getter.apply(context, args);
    cacheByArgs.set(key, result);
    return result;
  }
}

const resultCache = new ResultCache();

class Server {
  // persistent client state
  state = {
    serverErrors: [],
    queryStates: {},
  };

  setState(stateUpdate) {
    console.error('setState', Object.keys(stateUpdate));
    Object.assign(this.state, stateUpdate);
    io.emit('state', this.state);
  }

  handleError(message, error) {
    console.error(message, error);
    this.setState({
      serverErrors: this.state.serverErrors.concat({
        message,
        error: error.stack,
      }),
    });
  }

  async startQuery({sql, query}) {
    const res = await athena.runQuery(config.athena, sql);
    this.setState({
      queryStates: {
        ...this.state.queryStates,
        [res.QueryExecutionId]: {
          queryExecutionId: res.QueryExecutionId,
          sql,
          query,
          state: 'QUEUED',
        },
      },
    });
    return res.QueryExecutionId;
  }

  async getQueryStatus(queryExecutionId) {
    if (!queryExecutionId) throw new Error('missing queryExecutionId');
    const status = await athena.getQueryStatus(queryExecutionId);

    const queryState = {
      ...this.state.queryStates[queryExecutionId],
      status,
      state: status.QueryExecution
        ? status.QueryExecution.Status.State
        : 'QUEUED',
    };

    this.setState({
      queryStates: {
        ...this.state.queryStates,
        [queryExecutionId]: queryState,
      },
    });
    return queryState;
  }

  // RPC from client
  async handleCommand(cmd, data) {
    try {
      switch (cmd) {
        case 'query': {
          const queryExecutionId = await this.startQuery(data);
          let done = false;
          let status;
          while (!done) {
            await delay(1000);
            status = await this.getQueryStatus(queryExecutionId);
            done = status.state === 'SUCCEEDED' || status.state === 'FAILED';
            console.log('query', queryExecutionId, status.state);
          }
          return;
        }
        case 'status': {
          return await this.getQueryStatus(data.queryExecutionId);
        }
      }
    } catch (err) {
      this.handleError(
        `error running RPC: ${cmd} with arg ${JSON.stringify(data)}`,
        err
      );
    }
  }

  attachClientHandlers(socket) {
    // send current state on connect
    socket.emit('state', this.state);

    // subscribe to handle commands send from client
    socket.on('cmd', ({cmd, data}) => {
      this.handleCommand(cmd, data);
    });
  }

  async startServer(httpPort) {
    server.listen(httpPort);

    app.get('/', (req, res) => {
      if (DEV) {
        res.redirect(301, `http://127.0.0.1:3000/?port=${httpPort}`);
      } else {
        res.sendFile(path.join(__dirname, uiRoot, 'index.html'));
      }
    });

    app.get('/query-result', (req, res) => {
      res.set('Access-Control-Allow-Origin', '*');

      resultCache
        .get(athena, athena.getQueryResultsCSV, config.athena, req.query.id)
        .then((result) => {
          res.header('Content-Type', 'text/csv');
          res.status(200).send(result);
        })
        .catch((err) => {
          res.status(503).type('text').send(err.stack);
        });
    });

    app.get('/schema', (req, res) => {
      res.set('Access-Control-Allow-Origin', '*');

      resultCache
        .get(athena, athena.getSchemaFields, config.athena.schema.table)
        .then((fieldsFromAPI) => {
          const schema = {
            ...config.athena.schema,
            fields: {...fieldsFromAPI, ...config.athena.schema.fields},
          };
          res.json(schema);
        })
        .catch((err) => {
          res.status(503).type('text').send(err.stack);
        });
    });

    io.on('connection', (socket) => {
      this.attachClientHandlers(socket);
    });

    console.log(`server running at http://127.0.0.1:${httpPort}`);
  }
}

(config.port ? Promise.resolve(config.port) : getPort())
  .then(async (httpPort) => {
    await new Server().startServer(httpPort);
    return httpPort;
  })
  .then(async (httpPort) => {
    if (!DEV) {
      return;
    }

    console.log('opening ui');

    spawn('npm', ['start'], {
      env: {
        ...process.env,
        BROWSER: 'none',
      },
    });
    setTimeout(() => {
      exec(`open http://127.0.0.1:${httpPort}/`);
    }, 1000);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
