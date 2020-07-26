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

const athena = require('./athena');

const DEV = process.env.NODE_ENV === 'development';

const uiRoot = 'dist';

const app = express();
const server = http.Server(app);
const io = socketio(server);
app.use(express.static(uiRoot));

function delay(time) {
  return new Promise((resolve) => setTimeout(resolve, time));
}

class Server {
  // persistent client state
  state = {
    serverErrors: [],
    queryStates: {},
  };

  setState(stateUpdate) {
    console.error('setState', Object.keys(stateUpdate));
    Object.assign(this.state, stateUpdate);
    fs.writeFile(
      'laststate.json',
      JSON.stringify(this.state),
      {encoding: 'utf8'},
      () => {}
    );
    io.emit('state', this.state);
  }

  handleError(message, error) {
    this.state.serverErrors.push({message, error});
    console.error(message, error);
  }

  async startQuery({sql, query}) {
    const res = await athena.runQuery(sql);
    this.setState({
      queryStates: {
        ...this.state.queryStates,
        [res.QueryExecutionId]: {sql, query},
      },
    });
    return res.QueryExecutionId;
  }

  async getQueryStatus(queryExecutionId) {
    if (!queryExecutionId) throw new Error('missing queryExecutionId');
    const status = await athena.getQueryStatus(queryExecutionId);

    this.setState({
      queryStates: {
        ...this.state.queryStates,
        [queryExecutionId]: {
          ...this.state.queryStates[queryExecutionId],
          status,
        },
      },
    });
    return status;
  }

  async handleCommand(cmd, data) {
    try {
      switch (cmd) {
        case 'query': {
          const queryExecutionId = await this.startQuery(data);
          let done = false;
          let status;
          while (!done) {
            await delay(500);
            status = await this.getQueryStatus(queryExecutionId);
            done =
              status.QueryExecution &&
              ['SUCCEEDED', 'FAILED'].includes(
                status.QueryExecution.Status.State
              );
          }
          return;
        }
        case 'status': {
          return await this.getQueryStatus(data.queryExecutionId);
        }
      }
    } catch (err) {
      this.handleError(err);
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

      athena
        .getQueryResultsCSV(req.query.id)
        .then((result) => {
          res.header('Content-Type', 'text/csv');
          res.status(200).send(result);
        })
        .catch((err) => {
          res
            .status(503)
            .type('text')
            .send(err.stack);
        });
    });

    io.on('connection', (socket) => {
      this.attachClientHandlers(socket);
    });

    console.log(`server running at http://127.0.0.1:${httpPort}`);
  }
}

getPort()
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
