'use strict';

// const cluster = require('../lib');
// const co = require('co');
const mm = require('mm');
const net = require('net');
const Base = require('sdk-base');
// const Base = require('sdk-base');
// const is = require('is-type-of');
// const assert = require('assert');
// const symbols = require('../lib/symbol');
// const sleep = require('mz-modules/sleep');
// const EventEmitter = require('events').EventEmitter;
const APIClientBase = require('..').APIClientBase;
// const DataClient = require('./supports/client');

describe('test/single.test.js', () => {
  let port;
  before(done => {
    const server = net.createServer();
    server.listen(0, () => {
      const address = server.address();
      port = address.port;
      console.log('using port =>', port);
      server.close();
      done();
    });
  });

  class DataClient extends Base {
    constructor(options) {
      super(options);
      this.ready(true);
    }

    subscribe(info, listener) {
      console.log('37', info);
      console.log('38', listener);
    }

    publish(info) {
      console.log('42', info);
    }

    * getData(id) {
      console.log('46', id);
    }
  }

  class APIClient extends APIClientBase {
    constructor(options) {
      super(options);
      this._cache = new Map();
    }
    get DataClient() {
      return DataClient;
    }
    get delegates() {
      return {
        getData: 'invoke',
      };
    }
    get clusterOptions() {
      return {
        name: 'MyClient',
      };
    }
    subscribe(...args) {
      return this._client.subscribe(...args);
    }
    publish(...args) {
      return this._client.publish(...args);
    }
    * getData(id) {
      // write your business logic & use data client API
      if (this._cache.has(id)) {
        return this._cache.get(id);
      }
      const data = yield this._client.getData(id);
      this._cache.set(id, data);
      return data;
    }
  }

  describe.only('single', () => {
    before(() => {
      mm(process.env, 'NODE_CLUSTER_CLIENT_SINGLE_MODE', '1');
    });
    after(mm.restore);
    it('should work ok', async function() {
      const client = new APIClient();

      const listener = val => {
        console.log('listener hear:', val);
      };

      client.subscribe({ key: 'test' }, listener);

      client.publish({ key: 'foo', value: 'bar' });
    });
  });
});
